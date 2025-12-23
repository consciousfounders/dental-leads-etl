#!/usr/bin/env python3
"""
License Event Detection Engine

Compares snapshots to detect changes and generate actionable events.
This is the core intelligence that powers marketing triggers.

Event Types:
- NEW_LICENSE: Someone just got licensed (highest value!)
- LAPSED: License cancelled/expired (suppress or win-back)
- REINSTATED: Came back from lapsed (re-engagement)
- ADDRESS_CHANGE: Moved location (territory update)
- NEW_CERTIFICATION: Got new cert (upsell opportunity)

Usage:
    python license_events.py --state TX --date 2024-12-23
    python license_events.py --state TX --compare-days 1
"""

import pandas as pd
import os
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class EventType(Enum):
    NEW_LICENSE = "NEW_LICENSE"
    LAPSED = "LAPSED"
    REINSTATED = "REINSTATED"
    ADDRESS_CHANGE = "ADDRESS_CHANGE"
    NEW_CERTIFICATION = "NEW_CERTIFICATION"
    STATUS_CHANGE = "STATUS_CHANGE"


@dataclass
class LicenseEvent:
    """A detected license change event."""
    event_id: str
    event_type: str
    timestamp: str
    state_code: str
    professional_type: str
    license_number: str
    first_name: str
    last_name: str
    city: Optional[str]
    county: Optional[str]
    zip_code: Optional[str]
    previous_value: Optional[str]
    current_value: Optional[str]
    priority: str  # HIGH, MEDIUM, LOW
    marketing_action: str
    raw_data: Dict[str, Any]

    def to_dict(self) -> Dict:
        return asdict(self)


class TexasEventDetector:
    """
    Detect events by comparing Texas license snapshots.
    """

    # Status codes that mean "active"
    ACTIVE_STATUSES = {20, 46, 70}  # Active, Active/Probate, Charity

    # Status codes that mean "lapsed"
    LAPSED_STATUSES = {45, 48, 60}  # Expired, Expired-NSF, Cancelled

    def __init__(self, data_dir: str = "data/licenses/texas"):
        self.data_dir = data_dir

    def get_available_dates(self) -> List[str]:
        """Get list of available snapshot dates."""
        dates = []
        for item in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, item)
            if os.path.isdir(path) and item not in ["current", "raw"]:
                try:
                    datetime.strptime(item, "%Y-%m-%d")
                    dates.append(item)
                except ValueError:
                    pass
        return sorted(dates)

    def load_snapshot(self, date: str, file_type: str = "dentist") -> Optional[pd.DataFrame]:
        """Load a specific snapshot."""
        # Try date directory first
        path = os.path.join(self.data_dir, date, f"{file_type}.csv")
        if os.path.exists(path):
            return pd.read_csv(path, dtype=str)

        # Try legacy format (flat files with date suffix)
        for f in os.listdir(self.data_dir):
            if f.startswith(f"tx_{file_type}") or f.startswith(file_type.capitalize()):
                if date.replace("-", "") in f or f == f"{file_type.capitalize()}.csv":
                    path = os.path.join(self.data_dir, f)
                    return pd.read_csv(path, dtype=str)

        # Try current directory
        path = os.path.join(self.data_dir, "current", f"{file_type}.csv")
        if os.path.exists(path):
            return pd.read_csv(path, dtype=str)

        return None

    def detect_new_licenses(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        professional_type: str = "dentist"
    ) -> List[LicenseEvent]:
        """
        Detect NEW_LICENSE events.

        These are license numbers that exist in current but not in previous.
        """
        events = []
        timestamp = datetime.now().isoformat()

        # Get license numbers in each
        current_licenses = set(current_df["LIC_NBR"].dropna().astype(str))
        previous_licenses = set(previous_df["LIC_NBR"].dropna().astype(str))

        # Find new ones
        new_licenses = current_licenses - previous_licenses

        # Get details for new licenses
        new_df = current_df[current_df["LIC_NBR"].astype(str).isin(new_licenses)]

        for _, row in new_df.iterrows():
            # Skip if not active
            status_code = int(row.get("LIC_STA_CDE", 0) or 0)
            if status_code not in self.ACTIVE_STATUSES:
                continue

            event = LicenseEvent(
                event_id=f"NEW_{row['LIC_NBR']}_{timestamp[:10]}",
                event_type=EventType.NEW_LICENSE.value,
                timestamp=timestamp,
                state_code="TX",
                professional_type=professional_type,
                license_number=str(row.get("LIC_NBR", "")),
                first_name=str(row.get("FIRST_NME", "")).strip(),
                last_name=str(row.get("LAST_NME", "") or row.get("LAST_MNE", "")).strip(),
                city=str(row.get("CITY", "")).strip() or None,
                county=str(row.get("COUNTY", "")).strip() or None,
                zip_code=str(row.get("ZIP", "")).strip()[:5] or None,
                previous_value=None,
                current_value=str(row.get("LIC_STA_DESC", "")),
                priority="HIGH",
                marketing_action="onboarding_sequence",
                raw_data=row.to_dict(),
            )
            events.append(event)

        return events

    def detect_status_changes(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        professional_type: str = "dentist"
    ) -> List[LicenseEvent]:
        """
        Detect LAPSED and REINSTATED events.
        """
        events = []
        timestamp = datetime.now().isoformat()

        # Merge on license number
        merged = pd.merge(
            previous_df[["LIC_NBR", "LIC_STA_CDE", "LIC_STA_DESC"]],
            current_df[["LIC_NBR", "LIC_STA_CDE", "LIC_STA_DESC", "FIRST_NME",
                       "LAST_NME", "CITY", "COUNTY", "ZIP"]],
            on="LIC_NBR",
            suffixes=("_prev", "_curr"),
            how="inner"
        )

        # Handle column name variations
        if "LAST_NME" not in merged.columns and "LAST_MNE" in current_df.columns:
            merged = pd.merge(
                previous_df[["LIC_NBR", "LIC_STA_CDE", "LIC_STA_DESC"]],
                current_df[["LIC_NBR", "LIC_STA_CDE", "LIC_STA_DESC", "FIRST_NME",
                           "LAST_MNE", "CITY", "COUNTY", "ZIP"]],
                on="LIC_NBR",
                suffixes=("_prev", "_curr"),
                how="inner"
            )
            merged["LAST_NME"] = merged["LAST_MNE"]

        for _, row in merged.iterrows():
            prev_status = int(row.get("LIC_STA_CDE_prev", 0) or 0)
            curr_status = int(row.get("LIC_STA_CDE_curr", 0) or 0)

            if prev_status == curr_status:
                continue

            # LAPSED: Was active, now lapsed
            if prev_status in self.ACTIVE_STATUSES and curr_status in self.LAPSED_STATUSES:
                event = LicenseEvent(
                    event_id=f"LAPSED_{row['LIC_NBR']}_{timestamp[:10]}",
                    event_type=EventType.LAPSED.value,
                    timestamp=timestamp,
                    state_code="TX",
                    professional_type=professional_type,
                    license_number=str(row["LIC_NBR"]),
                    first_name=str(row.get("FIRST_NME", "")).strip(),
                    last_name=str(row.get("LAST_NME", "")).strip(),
                    city=str(row.get("CITY", "")).strip() or None,
                    county=str(row.get("COUNTY", "")).strip() or None,
                    zip_code=str(row.get("ZIP", "")).strip()[:5] or None,
                    previous_value=str(row.get("LIC_STA_DESC_prev", "")),
                    current_value=str(row.get("LIC_STA_DESC_curr", "")),
                    priority="MEDIUM",
                    marketing_action="suppress_or_winback",
                    raw_data={},
                )
                events.append(event)

            # REINSTATED: Was lapsed, now active
            elif prev_status in self.LAPSED_STATUSES and curr_status in self.ACTIVE_STATUSES:
                event = LicenseEvent(
                    event_id=f"REINSTATED_{row['LIC_NBR']}_{timestamp[:10]}",
                    event_type=EventType.REINSTATED.value,
                    timestamp=timestamp,
                    state_code="TX",
                    professional_type=professional_type,
                    license_number=str(row["LIC_NBR"]),
                    first_name=str(row.get("FIRST_NME", "")).strip(),
                    last_name=str(row.get("LAST_NME", "")).strip(),
                    city=str(row.get("CITY", "")).strip() or None,
                    county=str(row.get("COUNTY", "")).strip() or None,
                    zip_code=str(row.get("ZIP", "")).strip()[:5] or None,
                    previous_value=str(row.get("LIC_STA_DESC_prev", "")),
                    current_value=str(row.get("LIC_STA_DESC_curr", "")),
                    priority="HIGH",
                    marketing_action="reengagement_sequence",
                    raw_data={},
                )
                events.append(event)

        return events

    def detect_all_events(
        self,
        current_date: str,
        previous_date: str,
        professional_types: List[str] = None
    ) -> List[LicenseEvent]:
        """
        Run all event detection for given dates.
        """
        if professional_types is None:
            professional_types = ["dentist", "hygienist", "dental_assistant"]

        all_events = []

        for ptype in professional_types:
            # Map professional type to file name
            file_type = {
                "dentist": "dentist",
                "hygienist": "hygienist",
                "dental_assistant": "dental_assistant",
            }.get(ptype, ptype)

            print(f"\nProcessing {ptype}...")

            current_df = self.load_snapshot(current_date, file_type)
            previous_df = self.load_snapshot(previous_date, file_type)

            if current_df is None:
                print(f"  WARNING: No current snapshot found for {ptype}")
                continue
            if previous_df is None:
                print(f"  WARNING: No previous snapshot found for {ptype}")
                continue

            print(f"  Current: {len(current_df):,} records")
            print(f"  Previous: {len(previous_df):,} records")

            # Detect events
            new_events = self.detect_new_licenses(current_df, previous_df, ptype)
            print(f"  NEW_LICENSE events: {len(new_events)}")
            all_events.extend(new_events)

            status_events = self.detect_status_changes(current_df, previous_df, ptype)
            lapsed = [e for e in status_events if e.event_type == "LAPSED"]
            reinstated = [e for e in status_events if e.event_type == "REINSTATED"]
            print(f"  LAPSED events: {len(lapsed)}")
            print(f"  REINSTATED events: {len(reinstated)}")
            all_events.extend(status_events)

        return all_events


def save_events(events: List[LicenseEvent], output_path: str):
    """Save events to JSON file."""
    data = [e.to_dict() for e in events]
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"\nSaved {len(events)} events to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Detect license change events"
    )
    parser.add_argument(
        "--state",
        default="TX",
        help="State code"
    )
    parser.add_argument(
        "--data-dir",
        default="data/licenses/texas",
        help="Data directory"
    )
    parser.add_argument(
        "--current-date",
        default=None,
        help="Current snapshot date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--previous-date",
        default=None,
        help="Previous snapshot date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--compare-days",
        type=int,
        default=1,
        help="Compare to N days ago"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("License Event Detection")
    print("=" * 60)

    detector = TexasEventDetector(data_dir=args.data_dir)

    # Determine dates
    available_dates = detector.get_available_dates()
    print(f"Available snapshots: {available_dates}")

    if args.current_date:
        current_date = args.current_date
    elif available_dates:
        current_date = available_dates[-1]
    else:
        current_date = "current"

    if args.previous_date:
        previous_date = args.previous_date
    elif len(available_dates) >= 2:
        previous_date = available_dates[-2]
    else:
        # Use current as fallback (will show no changes)
        previous_date = current_date

    print(f"Comparing: {previous_date} → {current_date}")
    print("=" * 60)

    # Detect events
    events = detector.detect_all_events(current_date, previous_date)

    # Summary
    print("\n" + "=" * 60)
    print("Event Summary")
    print("=" * 60)

    by_type = {}
    for e in events:
        by_type[e.event_type] = by_type.get(e.event_type, 0) + 1

    for event_type, count in sorted(by_type.items()):
        print(f"  {event_type}: {count}")

    print(f"\nTotal events: {len(events)}")

    # Save if requested
    if args.output:
        save_events(events, args.output)
    else:
        # Default output
        output_path = f"events/tx_events_{current_date}.json"
        os.makedirs("events", exist_ok=True)
        save_events(events, output_path)

    # Print sample events
    if events:
        print("\n" + "=" * 60)
        print("Sample Events (first 5)")
        print("=" * 60)
        for e in events[:5]:
            print(f"\n{e.event_type}: {e.first_name} {e.last_name}")
            print(f"  License: {e.license_number}")
            print(f"  Location: {e.city}, {e.county}")
            print(f"  Action: {e.marketing_action}")

    return events


if __name__ == "__main__":
    main()
