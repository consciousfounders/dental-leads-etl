#!/usr/bin/env python3
"""
Fetch Florida dental professional license data from FL DOH MQA.

Florida has two data access methods:
1. MQA Data Download Portal (https://data-download.mqa.flhealthsource.gov/)
   - Requires free account registration
   - Provides bulk pipe-delimited ASCII files
   - Updated daily
   - Best for full data dumps

2. MQA Search Services (https://mqa-internet.doh.state.fl.us/MQASearchServices/)
   - No authentication required
   - Search API with CSV export
   - Good for targeted queries

This script uses method 2 (Search Services) for automated access without authentication.
For full bulk downloads, manually register at the Data Download Portal.

Usage:
    python fetch_florida.py
    python fetch_florida.py --output-dir /path/to/output
    python fetch_florida.py --active-only

Manual Bulk Download Instructions:
1. Go to https://data-download.mqa.flhealthsource.gov/
2. Create a free account (one-time)
3. Select profession: Dentist, Dental Hygienist, etc.
4. Download pipe-delimited files
5. Place in data/florida/raw/ directory
"""

import requests
import csv
import os
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import time

# MQA Search Services API endpoint
SEARCH_URL = "https://mqa-internet.doh.state.fl.us/MQASearchServices/api/HealthCareProvider/GetSearchResults"

# Florida dental profession codes
# Based on FL DOH MQA profession list
DENTAL_PROFESSIONS = {
    "DEN": {
        "code": "DEN",
        "name": "Dentist",
        "professional_type": "dentist",
        "board": "Board of Dentistry"
    },
    "DH": {
        "code": "DH",
        "name": "Dental Hygienist",
        "professional_type": "hygienist",
        "board": "Board of Dentistry"
    },
    "DN": {
        "code": "DN",
        "name": "Dental Laboratory",
        "professional_type": "dental_lab",
        "board": "Board of Dentistry"
    },
}

# License status codes
STATUS_MAP = {
    "Clear/Active": "ACTIVE",
    "Active": "ACTIVE",
    "Expired": "EXPIRED",
    "Null and Void": "VOID",
    "Revoked": "REVOKED",
    "Suspended": "SUSPENDED",
    "Voluntary Inactive": "INACTIVE",
    "Inactive": "INACTIVE",
    "Retired": "RETIRED",
}


def search_licenses(
    profession_code: str,
    county: Optional[str] = None,
    status: Optional[str] = None,
    page_size: int = 1000,
    max_pages: int = 100,
) -> List[Dict[str, Any]]:
    """
    Search for licenses using MQA Search Services API.

    Note: The MQA API requires some search criteria. We iterate through
    counties or use broad searches to get comprehensive data.

    Args:
        profession_code: FL profession code (e.g., "DEN", "DH")
        county: Optional county filter
        status: Optional status filter
        page_size: Records per page
        max_pages: Maximum pages to fetch

    Returns:
        List of license dictionaries
    """
    all_records = []

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for page in range(1, max_pages + 1):
        payload = {
            "ProfessionCode": profession_code,
            "County": county or "",
            "Status": status or "",
            "PageNumber": page,
            "PageSize": page_size,
        }

        try:
            response = requests.post(SEARCH_URL, json=payload, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()

            records = data.get("Results", [])
            if not records:
                break

            all_records.extend(records)
            print(f"  Page {page}: {len(records)} records (total: {len(all_records)})")

            if len(records) < page_size:
                break

            # Rate limiting
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"  Error on page {page}: {e}")
            break

    return all_records


def fetch_all_dental_licenses(
    active_only: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch all dental licenses by iterating through professions.

    Returns:
        Dictionary mapping profession code to list of records
    """
    results = {}

    for prof_code, prof_info in DENTAL_PROFESSIONS.items():
        print(f"\nFetching {prof_info['name']} ({prof_code})...")

        status_filter = "Clear/Active" if active_only else None
        records = search_licenses(
            profession_code=prof_code,
            status=status_filter,
        )

        results[prof_code] = records
        print(f"  Total {prof_info['name']}: {len(records)}")

    return results


def transform_record(record: Dict[str, Any], profession_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Transform FL MQA record to standardized schema.
    """
    # Parse name components
    full_name = record.get("Name", "")
    name_parts = full_name.split(", ")
    last_name = name_parts[0] if name_parts else ""
    first_name = name_parts[1].split(" ")[0] if len(name_parts) > 1 else ""
    middle_name = " ".join(name_parts[1].split(" ")[1:]) if len(name_parts) > 1 and len(name_parts[1].split(" ")) > 1 else ""

    # Parse address
    address = record.get("Address", "")
    city = record.get("City", "")
    state = record.get("State", "")
    zip_code = record.get("ZipCode", "")

    # Map status
    raw_status = record.get("Status", "")
    status_category = STATUS_MAP.get(raw_status, "UNKNOWN")

    return {
        "state_code": "FL",
        "professional_type": profession_info["professional_type"],
        "license_type": profession_info["name"],
        "license_number": record.get("LicenseNumber", ""),
        "status": raw_status,
        "status_category": status_category,
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        "full_name": full_name,
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "county": record.get("County", ""),
        "original_issue_date": record.get("OriginalIssueDate", ""),
        "expiration_date": record.get("ExpirationDate", ""),
        "last_renewal_date": record.get("LastRenewalDate", ""),
        "board": profession_info["board"],
    }


def save_to_csv(records: List[Dict[str, Any]], output_path: str) -> None:
    """Save records to CSV file."""
    if not records:
        print("No records to save.")
        return

    fieldnames = [
        "state_code",
        "professional_type",
        "license_type",
        "license_number",
        "status",
        "status_category",
        "first_name",
        "middle_name",
        "last_name",
        "full_name",
        "address",
        "city",
        "state",
        "zip_code",
        "county",
        "original_issue_date",
        "expiration_date",
        "last_renewal_date",
        "board",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Saved {len(records)} records to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Florida dental license data"
    )
    parser.add_argument(
        "--output-dir",
        default="data/florida/raw",
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
    print("Florida Dental License Data Fetch")
    print("=" * 60)
    print(f"Source: FL DOH MQA Search Services")
    print(f"Active only: {args.active_only}")
    print(f"Output dir: {args.output_dir}")
    print("=" * 60)
    print("\nNote: For complete bulk data, register at:")
    print("https://data-download.mqa.flhealthsource.gov/")
    print("=" * 60)

    # Fetch all dental licenses
    raw_results = fetch_all_dental_licenses(active_only=args.active_only)

    # Transform and combine all records
    all_transformed = []

    for prof_code, raw_records in raw_results.items():
        prof_info = DENTAL_PROFESSIONS[prof_code]
        transformed = [transform_record(r, prof_info) for r in raw_records]
        all_transformed.extend(transformed)

        # Save individual profession file
        if transformed:
            prof_path = os.path.join(
                args.output_dir,
                f"fl_{prof_info['professional_type']}_{timestamp}.csv"
            )
            save_to_csv(transformed, prof_path)

    # Save combined file
    if all_transformed:
        combined_path = os.path.join(args.output_dir, f"fl_dental_all_{timestamp}.csv")
        save_to_csv(all_transformed, combined_path)

    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(all_transformed)}")

    print("\nBy professional type:")
    by_type = {}
    for record in all_transformed:
        ptype = record["professional_type"]
        by_type[ptype] = by_type.get(ptype, 0) + 1
    for ptype, count in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {ptype}: {count}")

    print("\nBy status:")
    by_status = {}
    for record in all_transformed:
        status = record["status_category"]
        by_status[status] = by_status.get(status, 0) + 1
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
