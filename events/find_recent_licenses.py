#!/usr/bin/env python3
"""
Find Recently Licensed Professionals

Queries license data to find professionals licensed within a specified timeframe.
Generates NEW_LICENSE events for use with triggers.

Usage:
    python find_recent_licenses.py --state TX --days 180 --output events/tx_new_licensees.json
    python find_recent_licenses.py --state TX --days 30 --types dentist,hygienist
"""

import json
import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import hashlib


# State configurations
STATE_CONFIGS = {
    "TX": {
        "data_dir": "data/licenses/texas/current",
        "files": {
            "dentist": {
                "file": "dentist.csv",
                "date_field": "LIC_ORIG_DTE",
                "date_format": "%m/%d/%Y",
                "license_field": "LIC_NBR",
                "status_field": "LIC_STA_CDE",
                "active_statuses": [20, 46, 70],
                "first_name": "FIRST_NME",
                "last_name": "LAST_NME",
                "city": "CITY",
                "county": "COUNTY",
                "zip": "ZIP",
                "state": "STATE",
            },
            "hygienist": {
                "file": "hygienist.csv",
                "date_field": "LIC_ORIG_DTE",
                "date_format": "%m/%d/%Y",
                "license_field": "LIC_NBR",
                "status_field": "LIC_STA_CDE",
                "active_statuses": [20, 46, 70],
                "first_name": "FIRST_NME",
                "last_name": "LAST_MNE",  # Note: typo in source file
                "city": "CITY",
                "county": "COUNTY",
                "zip": "ZIP",
                "state": "STATE",
            },
            "dental_assistant": {
                "file": "dental_assistant.csv",
                "date_field": "LIC_ORIG_DTE",
                "date_format": "%m/%d/%Y",
                "license_field": "LIC_NBR",
                "status_field": "LIC_STA_CDE",
                "active_statuses": [20, 46],
                "first_name": "FIRST_NME",
                "last_name": "LAST_NME",
                "city": "CITY",
                "county": "COUNTY",
                "zip": "ZIP",
                "state": "STATE",
            },
        },
    },
}


def generate_event_id(state: str, license_number: str, event_type: str) -> str:
    """Generate deterministic event ID."""
    data = f"{state}-{license_number}-{event_type}"
    return hashlib.md5(data.encode()).hexdigest()[:12]


def find_recent_licenses(
    state_code: str,
    days: int,
    professional_types: Optional[List[str]] = None,
    counties: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Find licenses issued within the specified number of days.

    Args:
        state_code: State code (e.g., "TX")
        days: Number of days to look back
        professional_types: Filter to these types (e.g., ["dentist"])
        counties: Filter to these counties (e.g., ["HARRIS", "DALLAS"])

    Returns:
        List of NEW_LICENSE events
    """
    if state_code not in STATE_CONFIGS:
        raise ValueError(f"State {state_code} not configured")

    config = STATE_CONFIGS[state_code]
    cutoff_date = datetime.now() - timedelta(days=days)
    events = []

    # Determine which professional types to process
    types_to_process = professional_types or list(config["files"].keys())

    for ptype in types_to_process:
        if ptype not in config["files"]:
            print(f"  Warning: {ptype} not configured for {state_code}")
            continue

        file_config = config["files"][ptype]
        file_path = os.path.join(config["data_dir"], file_config["file"])

        if not os.path.exists(file_path):
            print(f"  Warning: {file_path} not found")
            continue

        print(f"  Processing {ptype}...")

        # Read CSV
        df = pd.read_csv(file_path, low_memory=False)

        # Parse date
        df["_license_date"] = pd.to_datetime(
            df[file_config["date_field"]],
            format=file_config["date_format"],
            errors="coerce"
        )

        # Filter by date
        recent = df[df["_license_date"] >= cutoff_date].copy()

        # Filter to active statuses
        if file_config.get("status_field") and file_config.get("active_statuses"):
            recent = recent[recent[file_config["status_field"]].isin(file_config["active_statuses"])]

        # Filter by county if specified
        if counties:
            counties_upper = [c.upper() for c in counties]
            recent = recent[recent[file_config.get("county", "COUNTY")].str.upper().isin(counties_upper)]

        print(f"    Found {len(recent)} recent licenses")

        # Convert to events
        for _, row in recent.iterrows():
            license_number = str(row[file_config["license_field"]])
            license_date = row["_license_date"]

            event = {
                "event_id": generate_event_id(state_code, license_number, "NEW_LICENSE"),
                "event_type": "NEW_LICENSE",
                "timestamp": license_date.isoformat() if pd.notna(license_date) else None,
                "state_code": state_code,
                "professional_type": ptype,
                "license_number": license_number,
                "first_name": str(row.get(file_config["first_name"], "")).strip(),
                "last_name": str(row.get(file_config["last_name"], "")).strip(),
                "city": str(row.get(file_config.get("city", "CITY"), "")).strip(),
                "county": str(row.get(file_config.get("county", "COUNTY"), "")).strip(),
                "zip_code": str(row.get(file_config.get("zip", "ZIP"), "")).strip(),
                "state": str(row.get(file_config.get("state", "STATE"), state_code)).strip(),
                "priority": "HIGH",
                "marketing_action": "new_licensee_campaign",
            }
            events.append(event)

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Find recently licensed professionals"
    )
    parser.add_argument(
        "--state",
        default="TX",
        help="State code (default: TX)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=180,
        help="Look back this many days (default: 180)"
    )
    parser.add_argument(
        "--types",
        default=None,
        help="Comma-separated professional types (e.g., 'dentist,hygienist')"
    )
    parser.add_argument(
        "--counties",
        default=None,
        help="Comma-separated counties to filter (e.g., 'HARRIS,DALLAS')"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file (default: events/{state}_new_licensees_{date}.json)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Find Recent Licenses")
    print("=" * 60)
    print(f"State: {args.state}")
    print(f"Days: {args.days}")

    # Parse filters
    professional_types = None
    if args.types:
        professional_types = [t.strip() for t in args.types.split(",")]
        print(f"Types: {professional_types}")

    counties = None
    if args.counties:
        counties = [c.strip() for c in args.counties.split(",")]
        print(f"Counties: {counties}")

    # Find events
    print()
    events = find_recent_licenses(
        args.state,
        args.days,
        professional_types=professional_types,
        counties=counties
    )

    print()
    print(f"Total events: {len(events)}")

    # Group by type
    by_type = {}
    for e in events:
        ptype = e["professional_type"]
        by_type[ptype] = by_type.get(ptype, 0) + 1

    print("By type:")
    for ptype, count in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {ptype}: {count}")

    # Save output
    if args.output:
        output_path = args.output
    else:
        os.makedirs("events", exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_path = f"events/{args.state.lower()}_new_licensees_{date_str}.json"

    with open(output_path, "w") as f:
        json.dump(events, f, indent=2, default=str)

    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()
