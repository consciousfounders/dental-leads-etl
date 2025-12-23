#!/usr/bin/env python3
"""
Fetch Colorado dental professional license data from data.colorado.gov
Source: https://data.colorado.gov/Regulations/Professional-and-Occupational-Licenses-in-Colorado/7s5z-vewr

This script pulls all dental-related licenses from Colorado DORA's open data portal.
The Socrata API supports unlimited exports via pagination.

Usage:
    python fetch_colorado.py
    python fetch_colorado.py --output-dir /path/to/output
    python fetch_colorado.py --active-only
"""

import requests
import csv
import os
import argparse
from datetime import datetime
from typing import List, Dict, Any

# Socrata Open Data API endpoint
BASE_URL = "https://data.colorado.gov/resource/7s5z-vewr.json"

# Colorado license type codes for dental professionals
# Based on DORA license type abbreviations
DENTAL_LICENSE_TYPES = {
    "DEN": "dentist",           # Dentist
    "T-DEN": "dentist",         # Temporary Dentist
    "MSDEN": "dentist",         # Military Spouse Dentist
    "DH": "hygienist",          # Dental Hygienist
    "T-DH": "hygienist",        # Temporary Dental Hygienist
    # Note: Colorado doesn't appear to have separate dental assistant licensing
    # They may fall under other categories or be unlicensed
}

# Full license type descriptions (for reference)
LICENSE_TYPE_DESCRIPTIONS = {
    "DEN": "Dentist",
    "T-DEN": "Temporary Dentist",
    "MSDEN": "Military Spouse Dentist",
    "DH": "Dental Hygienist",
    "T-DH": "Temporary Dental Hygienist",
}


def fetch_licenses(
    license_types: List[str],
    active_only: bool = False,
    limit: int = 50000
) -> List[Dict[str, Any]]:
    """
    Fetch licenses from Colorado DORA Socrata API.

    Args:
        license_types: List of license type codes to fetch
        active_only: If True, only fetch Active status records
        limit: Maximum records per API call

    Returns:
        List of license dictionaries
    """
    all_records = []

    # Build WHERE clause for license types
    type_conditions = " OR ".join([f"licensetype='{lt}'" for lt in license_types])
    where_clause = f"({type_conditions})"

    if active_only:
        where_clause += " AND licensestatusdescription='Active'"

    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$limit": limit,
            "$offset": offset,
            "$order": "licensenumber"
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
    """Convert ISO datetime to YYYY-MM-DD format."""
    if not date_str:
        return ""
    try:
        # Format: 2021-08-05T00:00:00.000
        return date_str[:10]
    except:
        return ""


def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Colorado record to standardized schema.
    """
    license_type = record.get("licensetype", "")

    return {
        "state_code": "CO",
        "professional_type": DENTAL_LICENSE_TYPES.get(license_type, "other"),
        "license_type_code": license_type,
        "license_type_description": LICENSE_TYPE_DESCRIPTIONS.get(license_type, license_type),
        "license_number": record.get("licensenumber", ""),
        "status": record.get("licensestatusdescription", ""),
        "first_name": record.get("firstname", ""),
        "middle_name": record.get("middlename", ""),
        "last_name": record.get("lastname", ""),
        "city": record.get("city", ""),
        "state": record.get("state", ""),
        "zip_code": record.get("mailzipcode", ""),
        "first_issue_date": parse_date(record.get("licensefirstissuedate", "")),
        "last_renewed_date": parse_date(record.get("licenselastreneweddate", "")),
        "expiration_date": parse_date(record.get("licenseexpirationdate", "")),
        "verification_url": record.get("linktoverifylicense", {}).get("url", "") if isinstance(record.get("linktoverifylicense"), dict) else "",
        "healthcare_profile_url": record.get("linktoviewhealthcareprofile", {}).get("url", "") if isinstance(record.get("linktoviewhealthcareprofile"), dict) else "",
    }


def save_to_csv(records: List[Dict[str, Any]], output_path: str) -> None:
    """Save records to CSV file."""
    if not records:
        print("No records to save.")
        return

    fieldnames = [
        "state_code",
        "professional_type",
        "license_type_code",
        "license_type_description",
        "license_number",
        "status",
        "first_name",
        "middle_name",
        "last_name",
        "city",
        "state",
        "zip_code",
        "first_issue_date",
        "last_renewed_date",
        "expiration_date",
        "verification_url",
        "healthcare_profile_url",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Saved {len(records)} records to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Colorado dental license data"
    )
    parser.add_argument(
        "--output-dir",
        default="data/colorado/raw",
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
    print("Colorado Dental License Data Fetch")
    print("=" * 60)
    print(f"Source: {BASE_URL}")
    print(f"Active only: {args.active_only}")
    print(f"Output dir: {args.output_dir}")
    print("=" * 60)

    # Fetch all dental licenses
    license_type_codes = list(DENTAL_LICENSE_TYPES.keys())
    print(f"\nFetching license types: {license_type_codes}")

    raw_records = fetch_licenses(
        license_type_codes,
        active_only=args.active_only
    )

    print(f"\nTotal raw records: {len(raw_records)}")

    # Transform records
    transformed = [transform_record(r) for r in raw_records]

    # Save combined file
    combined_path = os.path.join(args.output_dir, f"co_dental_all_{timestamp}.csv")
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
        type_path = os.path.join(args.output_dir, f"co_{ptype}_{timestamp}.csv")
        save_to_csv(records, type_path)

    # Print status breakdown
    print("\nRecords by status:")
    status_counts = {}
    for record in transformed:
        status = record["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")

    # Print license type breakdown
    print("\nRecords by license type:")
    type_counts = {}
    for record in transformed:
        lt = record["license_type_code"]
        type_counts[lt] = type_counts.get(lt, 0) + 1
    for lt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        desc = LICENSE_TYPE_DESCRIPTIONS.get(lt, lt)
        print(f"  {lt} ({desc}): {count}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
