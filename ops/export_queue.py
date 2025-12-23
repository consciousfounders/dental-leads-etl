#!/usr/bin/env python3
"""
Export Queue Management

Manages the export queue - adding records, approving, sending, and tracking.
Implements tiered approval based on destination cost and confidence.

Usage:
    # Queue new exports
    python export_queue.py queue --source golden_records.csv --destination ghl

    # Approve pending exports
    python export_queue.py approve --destination ghl --auto

    # Send approved exports
    python export_queue.py send --destination ghl --limit 100

    # Show queue status
    python export_queue.py status
"""

import argparse
import json
import hashlib
import os
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
import requests

import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class DestinationConfig:
    """Configuration for an export destination."""
    name: str
    display_name: str
    cost_per_record: float
    is_reversible: bool
    auto_approve: bool
    min_confidence_for_auto: int
    delay_hours: int
    rate_limit_per_hour: Optional[int] = None
    rate_limit_per_day: Optional[int] = None


DESTINATIONS = {
    'ghl': DestinationConfig(
        name='ghl',
        display_name='GoHighLevel CRM',
        cost_per_record=0,
        is_reversible=True,
        auto_approve=True,
        min_confidence_for_auto=70,
        delay_hours=0,
    ),
    'instantly': DestinationConfig(
        name='instantly',
        display_name='Instantly (Cold Email)',
        cost_per_record=0.01,
        is_reversible=False,
        auto_approve=True,
        min_confidence_for_auto=85,
        delay_hours=0,
        rate_limit_per_day=500,
    ),
    'lob_postcard': DestinationConfig(
        name='lob_postcard',
        display_name='Lob Postcard',
        cost_per_record=0.50,
        is_reversible=True,  # If not yet sent
        auto_approve=False,
        min_confidence_for_auto=95,
        delay_hours=24,
    ),
    'lob_letter': DestinationConfig(
        name='lob_letter',
        display_name='Lob Letter',
        cost_per_record=1.50,
        is_reversible=True,
        auto_approve=False,
        min_confidence_for_auto=95,
        delay_hours=48,
    ),
    'webhook': DestinationConfig(
        name='webhook',
        display_name='Custom Webhook',
        cost_per_record=0,
        is_reversible=False,
        auto_approve=True,
        min_confidence_for_auto=70,
        delay_hours=0,
    ),
}


# =============================================================================
# EXPORT QUEUE (file-based for local dev)
# =============================================================================

class ExportQueue:
    """File-based export queue for local development."""

    def __init__(self, queue_dir: str = "data/export_queue"):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.queue_file = self.queue_dir / "queue.json"
        self.history_file = self.queue_dir / "history.json"
        self.suppression_file = self.queue_dir / "suppression.json"

    def _load_json(self, path: Path) -> List[Dict]:
        if path.exists():
            with open(path, 'r') as f:
                return json.load(f)
        return []

    def _save_json(self, path: Path, data: List[Dict]):
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def _generate_export_id(self, provider_id: str, destination: str) -> str:
        data = f"{provider_id}-{destination}-{datetime.now().isoformat()}"
        return hashlib.md5(data.encode()).hexdigest()[:12]

    def _is_suppressed(self, record: Dict, destination: str) -> bool:
        """Check if record is on suppression list."""
        suppressions = self._load_json(self.suppression_file)
        for supp in suppressions:
            if not supp.get('is_active', True):
                continue
            if supp.get('destination') and supp['destination'] != destination:
                continue
            # Check matching fields
            if supp.get('email') and supp['email'].lower() == record.get('email', '').lower():
                return True
            if supp.get('license_number') and supp['license_number'] == record.get('license_number'):
                return True
            if supp.get('npi') and supp['npi'] == record.get('npi_number'):
                return True
        return False

    def add_to_queue(
        self,
        records: List[Dict],
        destination: str,
        load_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Add records to the export queue.

        Returns:
            Dict with counts: queued, auto_approved, suppressed, skipped
        """
        if destination not in DESTINATIONS:
            raise ValueError(f"Unknown destination: {destination}")

        config = DESTINATIONS[destination]
        queue = self._load_json(self.queue_file)

        counts = {
            'queued': 0,
            'auto_approved': 0,
            'suppressed': 0,
            'skipped': 0,
        }

        for record in records:
            provider_id = record.get('provider_id')
            if not provider_id:
                counts['skipped'] += 1
                continue

            # Check suppression
            if self._is_suppressed(record, destination):
                counts['suppressed'] += 1
                continue

            # Check if already in queue
            existing = [e for e in queue if e.get('provider_id') == provider_id
                        and e.get('destination') == destination
                        and e.get('status') in ['queued', 'approved', 'scheduled']]
            if existing:
                counts['skipped'] += 1
                continue

            # Calculate confidence
            match_confidence = record.get('match_confidence', 0)

            # Determine if auto-approve
            auto_approve = (
                config.auto_approve and
                match_confidence >= config.min_confidence_for_auto
            )

            # Calculate scheduled send time
            scheduled_send = None
            if config.delay_hours > 0:
                scheduled_send = datetime.now() + timedelta(hours=config.delay_hours)

            # Build export record
            export = {
                'export_id': self._generate_export_id(provider_id, destination),
                'provider_id': provider_id,
                'destination': destination,
                'payload': record,
                'data_load_id': load_id,
                'match_confidence': match_confidence,
                'requires_approval': not auto_approve,
                'status': 'approved' if auto_approve else 'queued',
                'approved_at': datetime.now().isoformat() if auto_approve else None,
                'approved_by': 'auto' if auto_approve else None,
                'scheduled_send_at': scheduled_send.isoformat() if scheduled_send else None,
                'queued_at': datetime.now().isoformat(),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
            }

            queue.append(export)
            counts['queued'] += 1
            if auto_approve:
                counts['auto_approved'] += 1

        self._save_json(self.queue_file, queue)
        return counts

    def get_pending_approval(self, destination: Optional[str] = None) -> List[Dict]:
        """Get exports awaiting approval."""
        queue = self._load_json(self.queue_file)
        pending = [e for e in queue if e.get('status') == 'queued']
        if destination:
            pending = [e for e in pending if e.get('destination') == destination]
        return pending

    def get_ready_to_send(self, destination: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get exports approved and ready to send."""
        queue = self._load_json(self.queue_file)
        now = datetime.now()

        ready = []
        for export in queue:
            if export.get('status') != 'approved':
                continue
            if destination and export.get('destination') != destination:
                continue

            # Check scheduled time
            scheduled = export.get('scheduled_send_at')
            if scheduled:
                scheduled_dt = datetime.fromisoformat(scheduled)
                if scheduled_dt > now:
                    continue

            ready.append(export)
            if len(ready) >= limit:
                break

        return ready

    def approve_exports(
        self,
        export_ids: Optional[List[str]] = None,
        destination: Optional[str] = None,
        min_confidence: int = 0,
        approver: str = 'manual',
    ) -> int:
        """Approve pending exports."""
        queue = self._load_json(self.queue_file)
        approved_count = 0

        for export in queue:
            if export.get('status') != 'queued':
                continue

            # Filter by IDs if provided
            if export_ids and export.get('export_id') not in export_ids:
                continue

            # Filter by destination
            if destination and export.get('destination') != destination:
                continue

            # Filter by confidence
            if export.get('match_confidence', 0) < min_confidence:
                continue

            # Approve
            export['status'] = 'approved'
            export['approved_at'] = datetime.now().isoformat()
            export['approved_by'] = approver
            export['updated_at'] = datetime.now().isoformat()
            approved_count += 1

        self._save_json(self.queue_file, queue)
        return approved_count

    def update_export(self, export_id: str, updates: Dict):
        """Update an export record."""
        queue = self._load_json(self.queue_file)
        for export in queue:
            if export.get('export_id') == export_id:
                export.update(updates)
                export['updated_at'] = datetime.now().isoformat()
                break
        self._save_json(self.queue_file, queue)

    def record_sent(self, export_id: str, external_id: Optional[str] = None, error: Optional[str] = None):
        """Record that an export was sent (or failed)."""
        queue = self._load_json(self.queue_file)
        history = self._load_json(self.history_file)

        for export in queue:
            if export.get('export_id') == export_id:
                if error:
                    export['status'] = 'failed'
                    export['error_message'] = error
                else:
                    export['status'] = 'sent'
                    export['sent_at'] = datetime.now().isoformat()
                    export['external_id'] = external_id

                    # Add to history
                    history.append({
                        'history_id': hashlib.md5(f"{export_id}-sent".encode()).hexdigest()[:12],
                        'export_id': export_id,
                        'provider_id': export.get('provider_id'),
                        'destination': export.get('destination'),
                        'payload': export.get('payload'),
                        'sent_at': export['sent_at'],
                        'external_id': external_id,
                        'estimated_cost': DESTINATIONS.get(export.get('destination'), {}).cost_per_record if hasattr(DESTINATIONS.get(export.get('destination'), {}), 'cost_per_record') else 0,
                    })

                export['updated_at'] = datetime.now().isoformat()
                break

        self._save_json(self.queue_file, queue)
        self._save_json(self.history_file, history)

    def add_suppression(
        self,
        email: Optional[str] = None,
        license_number: Optional[str] = None,
        npi: Optional[str] = None,
        destination: Optional[str] = None,
        reason: str = 'manual',
    ):
        """Add to suppression list."""
        suppressions = self._load_json(self.suppression_file)
        suppressions.append({
            'suppression_id': hashlib.md5(f"{email}-{license_number}-{npi}".encode()).hexdigest()[:12],
            'email': email,
            'license_number': license_number,
            'npi': npi,
            'destination': destination,
            'reason': reason,
            'is_active': True,
            'created_at': datetime.now().isoformat(),
        })
        self._save_json(self.suppression_file, suppressions)

    def get_status(self) -> Dict[str, Any]:
        """Get queue status summary."""
        queue = self._load_json(self.queue_file)
        history = self._load_json(self.history_file)

        status = {
            'total_in_queue': len(queue),
            'by_status': {},
            'by_destination': {},
            'pending_approval': 0,
            'ready_to_send': 0,
            'total_sent_today': 0,
            'total_sent_all_time': len(history),
        }

        today = datetime.now().date().isoformat()

        for export in queue:
            # By status
            s = export.get('status', 'unknown')
            status['by_status'][s] = status['by_status'].get(s, 0) + 1

            # By destination
            d = export.get('destination', 'unknown')
            if d not in status['by_destination']:
                status['by_destination'][d] = {'queued': 0, 'approved': 0, 'sent': 0, 'failed': 0}
            status['by_destination'][d][s] = status['by_destination'][d].get(s, 0) + 1

            if s == 'queued':
                status['pending_approval'] += 1
            elif s == 'approved':
                status['ready_to_send'] += 1

        # Sent today
        for h in history:
            if h.get('sent_at', '').startswith(today):
                status['total_sent_today'] += 1

        return status


# =============================================================================
# SENDERS
# =============================================================================

class GHLSender:
    """Send to GoHighLevel."""

    def __init__(self):
        self.api_key = os.environ.get('GHL_API_KEY')
        self.location_id = os.environ.get('GHL_LOCATION_ID')
        self.base_url = "https://rest.gohighlevel.com/v1"

    def send(self, export: Dict) -> tuple[Optional[str], Optional[str]]:
        """Send to GHL. Returns (external_id, error)."""
        if not self.api_key or not self.location_id:
            return None, "GHL_API_KEY or GHL_LOCATION_ID not configured"

        payload = export.get('payload', {})

        contact_data = {
            "locationId": self.location_id,
            "firstName": payload.get('first_name', ''),
            "lastName": payload.get('last_name', ''),
            "email": payload.get('email', ''),
            "phone": payload.get('phone', ''),
            "address1": payload.get('address1', ''),
            "city": payload.get('city', ''),
            "state": payload.get('state', ''),
            "postalCode": payload.get('zip_code', ''),
            "source": "License Pipeline",
            "tags": [
                f"license_{payload.get('professional_type', 'unknown')}",
                f"state_{payload.get('license_state', 'unknown')}",
            ],
        }

        if payload.get('is_new_licensee'):
            contact_data['tags'].append('new_licensee')

        try:
            response = requests.post(
                f"{self.base_url}/contacts/",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=contact_data,
                timeout=30,
            )

            if response.status_code in [200, 201]:
                result = response.json()
                return result.get('contact', {}).get('id'), None
            else:
                return None, f"HTTP {response.status_code}: {response.text[:200]}"

        except Exception as e:
            return None, str(e)


class WebhookSender:
    """Send to generic webhook."""

    def __init__(self, url: Optional[str] = None):
        self.url = url or os.environ.get('TRIGGER_WEBHOOK_URL')

    def send(self, export: Dict) -> tuple[Optional[str], Optional[str]]:
        if not self.url:
            return None, "TRIGGER_WEBHOOK_URL not configured"

        try:
            response = requests.post(
                self.url,
                json=export.get('payload', {}),
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code in [200, 201, 202]:
                return export.get('export_id'), None
            else:
                return None, f"HTTP {response.status_code}"

        except Exception as e:
            return None, str(e)


SENDERS = {
    'ghl': GHLSender,
    'webhook': WebhookSender,
    # Add more senders as needed
}


# =============================================================================
# CLI
# =============================================================================

def cmd_queue(args):
    """Queue records for export."""
    print(f"Queuing records from {args.source} to {args.destination}")

    # Load source data
    if args.source.endswith('.csv'):
        df = pd.read_csv(args.source)
        records = df.to_dict('records')
    elif args.source.endswith('.json'):
        with open(args.source, 'r') as f:
            records = json.load(f)
    else:
        print("Source must be .csv or .json")
        return

    queue = ExportQueue()
    counts = queue.add_to_queue(records, args.destination, load_id=args.load_id)

    print(f"\nResults:")
    print(f"  Queued: {counts['queued']}")
    print(f"  Auto-approved: {counts['auto_approved']}")
    print(f"  Suppressed: {counts['suppressed']}")
    print(f"  Skipped (duplicate): {counts['skipped']}")


def cmd_approve(args):
    """Approve pending exports."""
    queue = ExportQueue()

    if args.auto:
        # Auto-approve based on destination defaults
        config = DESTINATIONS.get(args.destination)
        if not config:
            print(f"Unknown destination: {args.destination}")
            return
        min_conf = config.min_confidence_for_auto
        approved = queue.approve_exports(
            destination=args.destination,
            min_confidence=min_conf,
            approver='auto-cli',
        )
        print(f"Auto-approved {approved} exports (min confidence: {min_conf})")
    else:
        # Show pending and prompt
        pending = queue.get_pending_approval(args.destination)
        print(f"Found {len(pending)} pending exports")

        if not pending:
            return

        for p in pending[:10]:
            payload = p.get('payload', {})
            print(f"  {p['export_id']}: {payload.get('first_name')} {payload.get('last_name')} "
                  f"(conf: {p.get('match_confidence', 0)})")

        if len(pending) > 10:
            print(f"  ... and {len(pending) - 10} more")

        confirm = input("\nApprove all? [y/N]: ")
        if confirm.lower() == 'y':
            approved = queue.approve_exports(destination=args.destination, approver='manual-cli')
            print(f"Approved {approved} exports")


def cmd_send(args):
    """Send approved exports."""
    queue = ExportQueue()

    ready = queue.get_ready_to_send(args.destination, limit=args.limit)
    print(f"Found {len(ready)} exports ready to send to {args.destination}")

    if not ready:
        return

    # Get sender
    sender_class = SENDERS.get(args.destination)
    if not sender_class:
        print(f"No sender configured for {args.destination}")
        return

    sender = sender_class()

    sent = 0
    failed = 0

    for export in ready:
        if args.dry_run:
            print(f"  [DRY RUN] Would send: {export.get('provider_id')}")
            continue

        external_id, error = sender.send(export)

        if error:
            queue.record_sent(export['export_id'], error=error)
            print(f"  [FAIL] {export.get('provider_id')}: {error}")
            failed += 1
        else:
            queue.record_sent(export['export_id'], external_id=external_id)
            print(f"  [OK] {export.get('provider_id')} -> {external_id}")
            sent += 1

    print(f"\nSent: {sent}, Failed: {failed}")


def cmd_status(args):
    """Show queue status."""
    queue = ExportQueue()
    status = queue.get_status()

    print("=" * 60)
    print("Export Queue Status")
    print("=" * 60)
    print(f"Total in queue: {status['total_in_queue']}")
    print(f"Pending approval: {status['pending_approval']}")
    print(f"Ready to send: {status['ready_to_send']}")
    print(f"Sent today: {status['total_sent_today']}")
    print(f"Sent all-time: {status['total_sent_all_time']}")
    print()

    if status['by_destination']:
        print("By Destination:")
        for dest, counts in status['by_destination'].items():
            config = DESTINATIONS.get(dest)
            display = config.display_name if config else dest
            print(f"  {display}:")
            for s, c in counts.items():
                if c > 0:
                    print(f"    {s}: {c}")


def main():
    parser = argparse.ArgumentParser(description="Export queue management")
    subparsers = parser.add_subparsers(dest='command', help='Command')

    # Queue command
    q_parser = subparsers.add_parser('queue', help='Queue records for export')
    q_parser.add_argument('--source', required=True, help='Source CSV or JSON file')
    q_parser.add_argument('--destination', required=True, choices=list(DESTINATIONS.keys()))
    q_parser.add_argument('--load-id', help='Data load ID for tracking')

    # Approve command
    a_parser = subparsers.add_parser('approve', help='Approve pending exports')
    a_parser.add_argument('--destination', required=True, choices=list(DESTINATIONS.keys()))
    a_parser.add_argument('--auto', action='store_true', help='Auto-approve based on confidence')

    # Send command
    s_parser = subparsers.add_parser('send', help='Send approved exports')
    s_parser.add_argument('--destination', required=True, choices=list(DESTINATIONS.keys()))
    s_parser.add_argument('--limit', type=int, default=100, help='Max records to send')
    s_parser.add_argument('--dry-run', action='store_true', help='Show what would be sent')

    # Status command
    subparsers.add_parser('status', help='Show queue status')

    args = parser.parse_args()

    if args.command == 'queue':
        cmd_queue(args)
    elif args.command == 'approve':
        cmd_approve(args)
    elif args.command == 'send':
        cmd_send(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
