#!/usr/bin/env python3
"""
Quarantine and Rollback Operations

Quarantine a bad data load and cascade effects to downstream systems.
Includes reversal of exports where possible.

Usage:
    python quarantine_load.py --load-id abc123 --reason "Corrupt source file"
    python quarantine_load.py --load-id abc123 --reason "Bad data" --reverse-exports
    python quarantine_load.py --list-quarantined
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import requests


@dataclass
class QuarantineResult:
    """Result of quarantine operation."""
    load_id: str
    quarantined_at: str
    reason: str
    exports_cancelled: int
    exports_reversed: int
    exports_failed_reversal: int
    reversal_failures: List[Dict]


# =============================================================================
# DESTINATION HANDLERS
# =============================================================================

class DestinationHandler:
    """Base class for destination-specific operations."""

    def __init__(self, config: Dict):
        self.config = config

    def reverse_export(self, export: Dict) -> bool:
        """Attempt to reverse/delete an export. Returns True if successful."""
        raise NotImplementedError

    def is_reversible(self) -> bool:
        return self.config.get('is_reversible', False)


class GHLHandler(DestinationHandler):
    """GoHighLevel handler."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_key = os.environ.get('GHL_API_KEY')
        self.base_url = "https://rest.gohighlevel.com/v1"

    def reverse_export(self, export: Dict) -> bool:
        """Delete contact from GHL."""
        if not self.api_key:
            print("    [WARN] GHL_API_KEY not set, skipping reversal")
            return False

        external_id = export.get('external_id')
        if not external_id:
            print("    [WARN] No external_id to reverse")
            return False

        try:
            response = requests.delete(
                f"{self.base_url}/contacts/{external_id}",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=30
            )
            if response.status_code in [200, 204]:
                return True
            else:
                print(f"    [ERROR] GHL delete failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"    [ERROR] GHL delete error: {e}")
            return False


class WebhookHandler(DestinationHandler):
    """Generic webhook handler - typically not reversible."""

    def reverse_export(self, export: Dict) -> bool:
        # Webhooks are typically fire-and-forget
        return False


class InstantlyHandler(DestinationHandler):
    """Instantly cold email handler - emails can't be unsent."""

    def reverse_export(self, export: Dict) -> bool:
        # Can't unsend emails
        return False


class LobHandler(DestinationHandler):
    """Lob mail handler - can cancel if not yet sent."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_key = os.environ.get('LOB_API_KEY')
        self.base_url = "https://api.lob.com/v1"

    def reverse_export(self, export: Dict) -> bool:
        """Cancel Lob mail if not yet sent."""
        if not self.api_key:
            print("    [WARN] LOB_API_KEY not set")
            return False

        external_id = export.get('external_id')
        if not external_id:
            return False

        try:
            response = requests.delete(
                f"{self.base_url}/postcards/{external_id}",
                auth=(self.api_key, ''),
                timeout=30
            )
            # Lob returns 200 if cancelled, 422 if already sent
            if response.status_code == 200:
                return True
            elif response.status_code == 422:
                print("    [WARN] Mail already sent, cannot cancel")
                return False
            else:
                print(f"    [ERROR] Lob cancel failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"    [ERROR] Lob cancel error: {e}")
            return False


DESTINATION_HANDLERS = {
    'ghl': GHLHandler({'is_reversible': True}),
    'webhook': WebhookHandler({'is_reversible': False}),
    'instantly': InstantlyHandler({'is_reversible': False}),
    'lob_postcard': LobHandler({'is_reversible': True}),  # If not yet sent
    'lob_letter': LobHandler({'is_reversible': True}),
}


# =============================================================================
# LOAD REGISTRY (file-based for now, would be Snowflake in production)
# =============================================================================

class LoadRegistry:
    """File-based load registry for local development."""

    def __init__(self, registry_dir: str = "data/registry"):
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.loads_file = self.registry_dir / "loads.json"
        self.exports_file = self.registry_dir / "exports.json"

    def _load_json(self, path: Path) -> List[Dict]:
        if path.exists():
            with open(path, 'r') as f:
                return json.load(f)
        return []

    def _save_json(self, path: Path, data: List[Dict]):
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def get_load(self, load_id: str) -> Optional[Dict]:
        loads = self._load_json(self.loads_file)
        for load in loads:
            if load.get('load_id') == load_id:
                return load
        return None

    def update_load(self, load_id: str, updates: Dict):
        loads = self._load_json(self.loads_file)
        for load in loads:
            if load.get('load_id') == load_id:
                load.update(updates)
                load['updated_at'] = datetime.now().isoformat()
                break
        self._save_json(self.loads_file, loads)

    def register_load(self, load: Dict):
        loads = self._load_json(self.loads_file)
        loads.append({
            **load,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
        })
        self._save_json(self.loads_file, loads)

    def get_exports_for_load(self, load_id: str, status: Optional[str] = None) -> List[Dict]:
        exports = self._load_json(self.exports_file)
        results = [e for e in exports if e.get('data_load_id') == load_id]
        if status:
            results = [e for e in results if e.get('status') == status]
        return results

    def update_export(self, export_id: str, updates: Dict):
        exports = self._load_json(self.exports_file)
        for export in exports:
            if export.get('export_id') == export_id:
                export.update(updates)
                export['updated_at'] = datetime.now().isoformat()
                break
        self._save_json(self.exports_file, exports)

    def get_quarantined_loads(self) -> List[Dict]:
        loads = self._load_json(self.loads_file)
        return [l for l in loads if l.get('status') == 'quarantined']


# =============================================================================
# QUARANTINE OPERATIONS
# =============================================================================

def quarantine_load(
    load_id: str,
    reason: str,
    reverse_exports: bool = False,
    registry: Optional[LoadRegistry] = None
) -> QuarantineResult:
    """
    Quarantine a data load and handle downstream effects.

    Args:
        load_id: ID of load to quarantine
        reason: Reason for quarantine
        reverse_exports: Whether to attempt reversing sent exports
        registry: Load registry instance

    Returns:
        QuarantineResult with details of actions taken
    """
    if registry is None:
        registry = LoadRegistry()

    print(f"Quarantining load: {load_id}")
    print(f"Reason: {reason}")
    print()

    # Update load status
    load = registry.get_load(load_id)
    if load:
        registry.update_load(load_id, {
            'status': 'quarantined',
            'quarantined_at': datetime.now().isoformat(),
            'quarantine_reason': reason,
        })
        print(f"[OK] Load marked as quarantined")
    else:
        print(f"[WARN] Load {load_id} not found in registry, creating entry")
        registry.register_load({
            'load_id': load_id,
            'status': 'quarantined',
            'quarantined_at': datetime.now().isoformat(),
            'quarantine_reason': reason,
        })

    # Cancel pending exports
    pending_statuses = ['queued', 'approved', 'scheduled']
    cancelled = 0

    print()
    print("Cancelling pending exports...")
    for status in pending_statuses:
        pending = registry.get_exports_for_load(load_id, status=status)
        for export in pending:
            registry.update_export(export['export_id'], {
                'status': 'cancelled',
                'error_message': f'Source load quarantined: {reason}',
            })
            cancelled += 1
            print(f"  [CANCEL] {export['export_id']} ({export.get('destination', 'unknown')})")

    print(f"Cancelled {cancelled} pending exports")

    # Reverse sent exports if requested
    reversed_count = 0
    failed_reversals = []

    if reverse_exports:
        print()
        print("Attempting to reverse sent exports...")
        sent_exports = registry.get_exports_for_load(load_id, status='sent')

        for export in sent_exports:
            destination = export.get('destination', 'unknown')
            handler = DESTINATION_HANDLERS.get(destination)

            if not handler:
                print(f"  [SKIP] {export['export_id']}: No handler for {destination}")
                continue

            if not handler.is_reversible():
                print(f"  [SKIP] {export['export_id']}: {destination} is not reversible")
                failed_reversals.append({
                    'export_id': export['export_id'],
                    'destination': destination,
                    'reason': 'Destination does not support reversal',
                })
                continue

            print(f"  Reversing {export['export_id']} from {destination}...")
            success = handler.reverse_export(export)

            if success:
                registry.update_export(export['export_id'], {
                    'reversed_at': datetime.now().isoformat(),
                    'reversal_reason': f'Source load quarantined: {reason}',
                })
                reversed_count += 1
                print(f"    [OK] Reversed")
            else:
                failed_reversals.append({
                    'export_id': export['export_id'],
                    'destination': destination,
                    'reason': 'Reversal failed',
                })
                print(f"    [FAIL] Could not reverse")

        print(f"Reversed {reversed_count} exports, {len(failed_reversals)} failed")

    # Update load with counts
    registry.update_load(load_id, {
        'exports_cancelled': cancelled,
        'exports_reversed': reversed_count,
    })

    return QuarantineResult(
        load_id=load_id,
        quarantined_at=datetime.now().isoformat(),
        reason=reason,
        exports_cancelled=cancelled,
        exports_reversed=reversed_count,
        exports_failed_reversal=len(failed_reversals),
        reversal_failures=failed_reversals,
    )


def list_quarantined(registry: Optional[LoadRegistry] = None):
    """List all quarantined loads."""
    if registry is None:
        registry = LoadRegistry()

    quarantined = registry.get_quarantined_loads()

    if not quarantined:
        print("No quarantined loads found.")
        return

    print("=" * 80)
    print("Quarantined Loads")
    print("=" * 80)

    for load in quarantined:
        print(f"\nLoad ID: {load.get('load_id')}")
        print(f"  Quarantined: {load.get('quarantined_at', 'unknown')}")
        print(f"  Reason: {load.get('quarantine_reason', 'unknown')}")
        print(f"  Source: {load.get('source_type', 'unknown')} - {load.get('source_file', 'unknown')}")
        print(f"  Exports cancelled: {load.get('exports_cancelled', 0)}")
        print(f"  Exports reversed: {load.get('exports_reversed', 0)}")


def main():
    parser = argparse.ArgumentParser(description="Quarantine and rollback operations")

    subparsers = parser.add_subparsers(dest='command', help='Command')

    # Quarantine command
    q_parser = subparsers.add_parser('quarantine', help='Quarantine a load')
    q_parser.add_argument('--load-id', required=True, help='Load ID to quarantine')
    q_parser.add_argument('--reason', required=True, help='Reason for quarantine')
    q_parser.add_argument('--reverse-exports', action='store_true',
                          help='Attempt to reverse sent exports')
    q_parser.add_argument('--output', help='Output JSON file for results')

    # List command
    l_parser = subparsers.add_parser('list', help='List quarantined loads')

    # Also support flat args for backward compatibility
    parser.add_argument('--load-id', help='Load ID to quarantine')
    parser.add_argument('--reason', help='Reason for quarantine')
    parser.add_argument('--reverse-exports', action='store_true')
    parser.add_argument('--list-quarantined', action='store_true')
    parser.add_argument('--output', help='Output JSON file')

    args = parser.parse_args()

    # Handle flat args
    if args.list_quarantined:
        list_quarantined()
        return

    if args.load_id and args.reason:
        result = quarantine_load(
            load_id=args.load_id,
            reason=args.reason,
            reverse_exports=args.reverse_exports,
        )

        print()
        print("=" * 60)
        print("Quarantine Complete")
        print("=" * 60)
        print(f"Load ID: {result.load_id}")
        print(f"Exports cancelled: {result.exports_cancelled}")
        print(f"Exports reversed: {result.exports_reversed}")
        print(f"Failed reversals: {result.exports_failed_reversal}")

        if result.reversal_failures:
            print("\nManual follow-up required:")
            for failure in result.reversal_failures:
                print(f"  - {failure['export_id']} ({failure['destination']}): {failure['reason']}")

        if args.output:
            with open(args.output, 'w') as f:
                json.dump(asdict(result), f, indent=2)
            print(f"\nResults saved to: {args.output}")

        return

    # Handle subcommands
    if args.command == 'quarantine':
        result = quarantine_load(
            load_id=args.load_id,
            reason=args.reason,
            reverse_exports=args.reverse_exports,
        )
        print(f"\nQuarantine complete: {result.exports_cancelled} cancelled, {result.exports_reversed} reversed")

    elif args.command == 'list':
        list_quarantined()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
