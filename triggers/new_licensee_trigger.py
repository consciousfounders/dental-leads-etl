#!/usr/bin/env python3
"""
New Licensee Trigger

Takes NEW_LICENSE events and pushes them to:
1. GoHighLevel (GHL) - Create/update contact
2. Webhook - Generic HTTP endpoint
3. Google Sheet - For manual review
4. Slack - Notification

This is the money-maker: "Dr. Smith just got licensed in Houston 3 days ago"

Usage:
    python new_licensee_trigger.py --events events/tx_events_2024-12-23.json
    python new_licensee_trigger.py --events events/tx_events_2024-12-23.json --dry-run
"""

import json
import os
import argparse
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class TriggerConfig:
    """Configuration for trigger destinations."""
    ghl_api_key: Optional[str] = None
    ghl_location_id: Optional[str] = None
    webhook_url: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    google_sheet_id: Optional[str] = None


class GHLClient:
    """GoHighLevel API client for contact management."""

    BASE_URL = "https://rest.gohighlevel.com/v1"

    def __init__(self, api_key: str, location_id: str):
        self.api_key = api_key
        self.location_id = location_id
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def create_contact(self, event: Dict) -> Dict:
        """Create a contact in GHL from a license event."""
        payload = {
            "locationId": self.location_id,
            "firstName": event.get("first_name", ""),
            "lastName": event.get("last_name", ""),
            "email": "",  # Would need enrichment
            "phone": "",  # Would need enrichment
            "address1": "",
            "city": event.get("city", ""),
            "state": event.get("state_code", "TX"),
            "postalCode": event.get("zip_code", ""),
            "source": "License Pipeline",
            "tags": [
                "new_licensee",
                f"license_{event.get('professional_type', 'dentist')}",
                f"county_{event.get('county', 'unknown')}",
            ],
            "customField": {
                "license_number": event.get("license_number", ""),
                "license_date": event.get("timestamp", "")[:10],
                "professional_type": event.get("professional_type", ""),
                "county": event.get("county", ""),
            },
        }

        response = requests.post(
            f"{self.BASE_URL}/contacts/",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        return response.json()

    def add_to_campaign(self, contact_id: str, campaign_id: str) -> Dict:
        """Add contact to a GHL campaign/workflow."""
        payload = {
            "contactId": contact_id,
        }
        response = requests.post(
            f"{self.BASE_URL}/campaigns/{campaign_id}/contacts/",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        return response.json()


def send_to_webhook(event: Dict, webhook_url: str) -> bool:
    """Send event to a generic webhook endpoint."""
    try:
        response = requests.post(
            webhook_url,
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        return response.status_code in [200, 201, 202]
    except Exception as e:
        print(f"  Webhook error: {e}")
        return False


def send_to_slack(events: List[Dict], webhook_url: str) -> bool:
    """Send summary notification to Slack."""
    if not events:
        return True

    # Group by professional type
    by_type = {}
    for e in events:
        ptype = e.get("professional_type", "unknown")
        by_type[ptype] = by_type.get(ptype, 0) + 1

    # Build message
    lines = [
        f"🦷 *{len(events)} New Texas Licensees Detected*",
        "",
    ]
    for ptype, count in sorted(by_type.items(), key=lambda x: -x[1]):
        lines.append(f"• {ptype.replace('_', ' ').title()}: {count}")

    lines.extend([
        "",
        "*Sample (first 3):*",
    ])

    for e in events[:3]:
        name = f"{e.get('first_name', '')} {e.get('last_name', '')}"
        city = e.get("city", "Unknown")
        lines.append(f"• {name} - {city}")

    payload = {
        "text": "\n".join(lines),
        "unfurl_links": False,
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"  Slack error: {e}")
        return False


def append_to_google_sheet(events: List[Dict], sheet_id: str) -> bool:
    """
    Append events to Google Sheet.

    Note: Requires Google Sheets API setup.
    For MVP, you could use a simple CSV append or Zapier integration.
    """
    # Placeholder - would use Google Sheets API
    print(f"  Google Sheets integration not yet implemented")
    return False


def load_events(events_file: str) -> List[Dict]:
    """Load events from JSON file."""
    with open(events_file, "r") as f:
        return json.load(f)


def filter_new_licensee_events(events: List[Dict]) -> List[Dict]:
    """Filter to only NEW_LICENSE events."""
    return [e for e in events if e.get("event_type") == "NEW_LICENSE"]


def run_triggers(
    events: List[Dict],
    config: TriggerConfig,
    dry_run: bool = False,
    professional_types: List[str] = None,
    counties: List[str] = None,
) -> Dict[str, Any]:
    """
    Run all configured triggers for the given events.

    Args:
        events: List of event dictionaries
        config: Trigger configuration
        dry_run: If True, don't actually send anything
        professional_types: Filter to these types (e.g., ["dentist"])
        counties: Filter to these counties (e.g., ["HARRIS", "DALLAS"])

    Returns:
        Summary of results
    """
    results = {
        "total_events": len(events),
        "filtered_events": 0,
        "ghl_created": 0,
        "ghl_errors": 0,
        "webhook_sent": 0,
        "webhook_errors": 0,
        "dry_run": dry_run,
    }

    # Filter events
    filtered = events
    if professional_types:
        filtered = [e for e in filtered if e.get("professional_type") in professional_types]
    if counties:
        counties_upper = [c.upper() for c in counties]
        filtered = [e for e in filtered if (e.get("county") or "").upper() in counties_upper]

    results["filtered_events"] = len(filtered)

    if not filtered:
        print("No events match filters")
        return results

    print(f"\nProcessing {len(filtered)} events...")

    # GHL Integration
    if config.ghl_api_key and config.ghl_location_id:
        print("\n→ GoHighLevel")
        ghl = GHLClient(config.ghl_api_key, config.ghl_location_id)

        for event in filtered:
            name = f"{event.get('first_name', '')} {event.get('last_name', '')}"
            if dry_run:
                print(f"  [DRY RUN] Would create contact: {name}")
                results["ghl_created"] += 1
            else:
                try:
                    result = ghl.create_contact(event)
                    if result.get("contact", {}).get("id"):
                        print(f"  ✓ Created: {name}")
                        results["ghl_created"] += 1
                    else:
                        print(f"  ✗ Failed: {name} - {result}")
                        results["ghl_errors"] += 1
                except Exception as e:
                    print(f"  ✗ Error: {name} - {e}")
                    results["ghl_errors"] += 1

    # Webhook Integration
    if config.webhook_url:
        print("\n→ Webhook")
        for event in filtered:
            name = f"{event.get('first_name', '')} {event.get('last_name', '')}"
            if dry_run:
                print(f"  [DRY RUN] Would send: {name}")
                results["webhook_sent"] += 1
            else:
                if send_to_webhook(event, config.webhook_url):
                    print(f"  ✓ Sent: {name}")
                    results["webhook_sent"] += 1
                else:
                    print(f"  ✗ Failed: {name}")
                    results["webhook_errors"] += 1

    # Slack Notification (summary, not per-event)
    if config.slack_webhook_url:
        print("\n→ Slack")
        if dry_run:
            print(f"  [DRY RUN] Would send summary for {len(filtered)} events")
        else:
            if send_to_slack(filtered, config.slack_webhook_url):
                print(f"  ✓ Summary sent")
            else:
                print(f"  ✗ Failed to send")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Trigger actions for new licensee events"
    )
    parser.add_argument(
        "--events",
        required=True,
        help="Path to events JSON file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually send, just show what would happen"
    )
    parser.add_argument(
        "--professional-types",
        default=None,
        help="Comma-separated list of types to include (e.g., 'dentist,hygienist')"
    )
    parser.add_argument(
        "--counties",
        default=None,
        help="Comma-separated list of counties to include (e.g., 'HARRIS,DALLAS,TARRANT')"
    )
    parser.add_argument(
        "--ghl-api-key",
        default=os.environ.get("GHL_API_KEY"),
        help="GoHighLevel API key"
    )
    parser.add_argument(
        "--ghl-location-id",
        default=os.environ.get("GHL_LOCATION_ID"),
        help="GoHighLevel location ID"
    )
    parser.add_argument(
        "--webhook-url",
        default=os.environ.get("TRIGGER_WEBHOOK_URL"),
        help="Generic webhook URL"
    )
    parser.add_argument(
        "--slack-webhook-url",
        default=os.environ.get("SLACK_WEBHOOK_URL"),
        help="Slack webhook URL for notifications"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("New Licensee Trigger")
    print("=" * 60)

    # Load events
    all_events = load_events(args.events)
    print(f"Loaded {len(all_events)} total events")

    # Filter to NEW_LICENSE only
    new_licensee_events = filter_new_licensee_events(all_events)
    print(f"NEW_LICENSE events: {len(new_licensee_events)}")

    if not new_licensee_events:
        print("\nNo new licensee events to process")
        return

    # Build config
    config = TriggerConfig(
        ghl_api_key=args.ghl_api_key,
        ghl_location_id=args.ghl_location_id,
        webhook_url=args.webhook_url,
        slack_webhook_url=args.slack_webhook_url,
    )

    # Parse filters
    professional_types = None
    if args.professional_types:
        professional_types = [t.strip() for t in args.professional_types.split(",")]

    counties = None
    if args.counties:
        counties = [c.strip() for c in args.counties.split(",")]

    # Show config
    print(f"\nConfiguration:")
    print(f"  Dry run: {args.dry_run}")
    print(f"  GHL: {'configured' if config.ghl_api_key else 'not configured'}")
    print(f"  Webhook: {'configured' if config.webhook_url else 'not configured'}")
    print(f"  Slack: {'configured' if config.slack_webhook_url else 'not configured'}")
    if professional_types:
        print(f"  Filter types: {professional_types}")
    if counties:
        print(f"  Filter counties: {counties}")

    # Run triggers
    print("\n" + "=" * 60)
    results = run_triggers(
        new_licensee_events,
        config,
        dry_run=args.dry_run,
        professional_types=professional_types,
        counties=counties,
    )

    # Summary
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    print(f"Total events: {results['total_events']}")
    print(f"After filters: {results['filtered_events']}")
    if config.ghl_api_key:
        print(f"GHL contacts created: {results['ghl_created']}")
        print(f"GHL errors: {results['ghl_errors']}")
    if config.webhook_url:
        print(f"Webhooks sent: {results['webhook_sent']}")
        print(f"Webhook errors: {results['webhook_errors']}")


if __name__ == "__main__":
    main()
