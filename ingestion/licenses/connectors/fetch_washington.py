#!/usr/bin/env python3
"""
Fetch Washington State dental professional license data from data.wa.gov
Source: https://data.wa.gov/health/Health-Care-Provider-Credential-Data/qxh8-f4bd

This script pulls all dental-related credentials from Washington DOH's open data portal.
The Socrata API supports unlimited exports via pagination.

Usage:
    python fetch_washington.py
    python fetch_washington.py --output-dir /path/to/output
    python fetch_washington.py --active-only
"""

import requests
import csv
import os
import argparse
from datetime import datetime
from typing import List, Dict, Any

# Socrata Open Data API endpoint
BASE_URL = "https://data.wa.gov/resource/qxh8-f4bd.json"

# Dental-related credential types to fetch
DENTAL_CREDENTIAL_TYPES = [
    "Dentist License",
    "Dentist Resident Postdoctoral License",
    "Dentist Resident Community License",
    "Dentist Faculty License",
    "Dentist Temporary Practice Permit",
    "Dentist Pediatric Sedation Endorsement",
    "Dentist Moderate Sedation Permit",
    "Dentist Moderate Sedation with Parenteral Agents Permit",
    "Dentist General Anesthesia Permit",
    "Dental Hygiene License",
    "Dental Hygiene Temporary Practice Permit",
    "Dental Hygiene Initial Temporary License",
    "Dental Hygiene Renewable Temporary License",
    "Dental Hygiene Initial Limited Temporary Practice Permit",
    "Dental Assistant Registration",
    "Dental Assistant Endorsement",
    "Dental Anesthesia Assistant Certification",
    "Expanded Function Dental Auxiliary",
    "Denturist License",
    "Denturist Temporary Practice Permit",
    "Denturist Alternate Location",
]

# Map WA credential types to our standard professional types
CREDENTIAL_TYPE_MAP = {
    "Dentist License": "dentist",
    "Dentist Resident Postdoctoral License": "dentist",
    "Dentist Resident Community License": "dentist",
    "Dentist Faculty License": "dentist",
    "Dentist Temporary Practice Permit": "dentist",
    "Dental Hygiene License": "hygienist",
    "Dental Hygiene Temporary Practice Permit": "hygienist",
    "Dental Hygiene Initial Temporary License": "hygienist",
    "Dental Hygiene Renewable Temporary License": "hygienist",
    "Dental Hygiene Initial Limited Temporary Practice Permit": "hygienist",
    "Dental Assistant Registration": "dental_assistant",
    "Dental Assistant Endorsement": "dental_assistant",
    "Dental Anesthesia Assistant Certification": "dental_assistant",
    "Expanded Function Dental Auxiliary": "dental_assistant",
    "Denturist License": "denturist",
    "Denturist Temporary Practice Permit": "denturist",
    "Denturist Alternate Location": "denturist",
}


def fetch_credentials(
    credential_types: List[str],
    active_only: bool = False,
    limit: int = 50000
) -> List[Dict[str, Any]]:
    """
    Fetch credentials from WA DOH Socrata API.

    Args:
        credential_types: List of credential type strings to fetch
        active_only: If True, only fetch ACTIVE status records
        limit: Maximum records per API call (Socrata default max is 50000)

    Returns:
        List of credential dictionaries
    """
    all_records = []

    # Build WHERE clause for credential types
    type_conditions = " OR ".join([f"credentialtype='{ct}'" for ct in credential_types])
    where_clause = f"({type_conditions})"

    if active_only:
        where_clause += " AND status='ACTIVE'"

    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$limit": limit,
            "$offset": offset,
            "$order": "credentialnumber"
        }

        print(f"Fetching records {offset} to {offset + limit}...")

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        records = response.json()

        if not records:
            break

        all_records.extend(records)

        if len(records) < limit:
            break

        offset += limit

    return all_records


def parse_date(date_str: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD format."""
    if not date_str or len(date_str) != 8:
        return ""
    try:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except:
        return ""


def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform WA record to standardized schema.
    """
    credential_type = record.get("credentialtype", "")

    return {
        "state_code": "WA",
        "professional_type": CREDENTIAL_TYPE_MAP.get(credential_type, "other"),
        "credential_type": credential_type,
        "license_number": record.get("credentialnumber", ""),
        "status": record.get("status", ""),
        "first_name": record.get("firstname", ""),
        "middle_name": record.get("middlename", ""),
        "last_name": record.get("lastname", ""),
        "birth_year": record.get("birthyear", ""),
        "first_issue_date": parse_date(record.get("firstissuedate", "")),
        "last_issue_date": parse_date(record.get("lastissuedate", "")),
        "expiration_date": parse_date(record.get("expirationdate", "")),
        "ce_due_date": parse_date(record.get("ceduedate", "")),
        "has_enforcement_action": record.get("actiontaken", "No") == "Yes",
    }


def save_to_csv(records: List[Dict[str, Any]], output_path: str) -> None:
    """Save records to CSV file."""
    if not records:
        print("No records to save.")
        return

    fieldnames = [
        "state_code",
        "professional_type",
        "credential_type",
        "license_number",
        "status",
        "first_name",
        "middle_name",
        "last_name",
        "birth_year",
        "first_issue_date",
        "last_issue_date",
        "expiration_date",
        "ce_due_date",
        "has_enforcement_action",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Saved {len(records)} records to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Washington State dental license data"
    )
    parser.add_argument(
        "--output-dir",
        default="data/washington/raw",
        help="Output directory for CSV files"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only fetch active licenses"
    )

    args = parser.parse_args()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print("Washington State Dental License Data Fetch")
    print("=" * 60)
    print(f"Source: {BASE_URL}")
    print(f"Active only: {args.active_only}")
    print(f"Output dir: {args.output_dir}")
    print("=" * 60)

    # Fetch all dental credentials
    print(f"\nFetching {len(DENTAL_CREDENTIAL_TYPES)} credential types...")
    raw_records = fetch_credentials(
        DENTAL_CREDENTIAL_TYPES,
        active_only=args.active_only
    )

    print(f"\nTotal raw records: {len(raw_records)}")

    # Transform records
    transformed = [transform_record(r) for r in raw_records]

    # Save combined file
    combined_path = os.path.join(args.output_dir, f"wa_dental_all_{timestamp}.csv")
    save_to_csv(transformed, combined_path)

    # Save by professional type
    by_type = {}
    for record in transformed:
        ptype = record["professional_type"]
        if ptype not in by_type:
            by_type[ptype] = []
        by_type[ptype].append(record)

    print("\nRecords by professional type:")
    for ptype, records in sorted(by_type.items()):
        print(f"  {ptype}: {len(records)}")
        type_path = os.path.join(args.output_dir, f"wa_{ptype}_{timestamp}.csv")
        save_to_csv(records, type_path)

    # Print status breakdown
    print("\nRecords by status:")
    status_counts = {}
    for record in transformed:
        status = record["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
