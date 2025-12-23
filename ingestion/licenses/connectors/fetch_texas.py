#!/usr/bin/env python3
"""
Fetch Texas dental professional license data from TSBDE.
Source: https://tsbde.texas.gov/resources/licensee-lists/

This connector downloads the daily CSV exports and saves them with date versioning
for historical tracking and change detection.

Usage:
    python fetch_texas.py
    python fetch_texas.py --output-dir /path/to/output
"""

import requests
import os
import argparse
from datetime import datetime
from typing import Dict, List, Tuple
import hashlib

# Texas State Board of Dental Examiners CSV endpoints
TEXAS_SOURCES = {
    "dentist": {
        "url": "https://ls.tsbde.texas.gov/lib/csv/Dentist.csv",
        "professional_type": "dentist",
    },
    "hygienist": {
        "url": "https://ls.tsbde.texas.gov/lib/csv/Hygienist.csv",
        "professional_type": "hygienist",
    },
    "dental_assistant": {
        "url": "https://ls.tsbde.texas.gov/lib/csv/DentalAssistant.csv",
        "professional_type": "dental_assistant",
    },
    "labs": {
        "url": "https://ls.tsbde.texas.gov/lib/csv/Labs.csv",
        "professional_type": "dental_lab",
    },
    "etn": {
        "url": "https://ls.tsbde.texas.gov/lib/csv/ETN.csv",
        "professional_type": "etn",
    },
}


def get_file_hash(filepath: str) -> str:
    """Calculate MD5 hash of a file."""
    if not os.path.exists(filepath):
        return ""
    with open(filepath, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def download_csv(url: str, output_path: str, timeout: int = 120) -> Tuple[bool, int]:
    """
    Download CSV from URL.

    Returns:
        Tuple of (success, bytes_downloaded)
    """
    try:
        print(f"  Downloading {url}...")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(response.content)

        return True, len(response.content)
    except Exception as e:
        print(f"  ERROR: {e}")
        return False, 0


def count_csv_rows(filepath: str) -> int:
    """Count rows in CSV (excluding header)."""
    try:
        with open(filepath, "r") as f:
            return sum(1 for _ in f) - 1  # Subtract header
    except:
        return 0


def fetch_texas(output_dir: str) -> Dict:
    """
    Fetch all Texas license data with date-versioned snapshots.

    Directory structure:
        output_dir/
        ├── current/           # Latest version (symlinks or copies)
        │   ├── dentist.csv
        │   └── ...
        ├── 2024-12-23/        # Date-versioned snapshots
        │   ├── dentist.csv
        │   └── ...
        └── 2024-12-24/
            └── ...
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # Create directories
    current_dir = os.path.join(output_dir, "current")
    snapshot_dir = os.path.join(output_dir, today)
    os.makedirs(current_dir, exist_ok=True)
    os.makedirs(snapshot_dir, exist_ok=True)

    results = {
        "date": today,
        "state": "TX",
        "files": [],
        "errors": [],
        "unchanged": [],
        "total_records": 0,
    }

    for source_key, source_info in TEXAS_SOURCES.items():
        filename = f"{source_key}.csv"
        snapshot_path = os.path.join(snapshot_dir, filename)
        current_path = os.path.join(current_dir, filename)

        # Download to snapshot directory
        success, size = download_csv(source_info["url"], snapshot_path)

        if success:
            row_count = count_csv_rows(snapshot_path)
            new_hash = get_file_hash(snapshot_path)
            old_hash = get_file_hash(current_path)

            # Check if data changed
            if new_hash == old_hash:
                results["unchanged"].append(source_key)
                print(f"  ✓ {source_key}: {row_count:,} rows (unchanged)")
            else:
                # Copy to current
                with open(snapshot_path, "rb") as src:
                    with open(current_path, "wb") as dst:
                        dst.write(src.read())
                print(f"  ✓ {source_key}: {row_count:,} rows ({size/1024/1024:.1f} MB) - UPDATED")

            results["files"].append({
                "source": source_key,
                "path": snapshot_path,
                "rows": row_count,
                "size": size,
                "hash": new_hash,
                "changed": new_hash != old_hash,
            })
            results["total_records"] += row_count
        else:
            results["errors"].append(source_key)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Texas dental license data"
    )
    parser.add_argument(
        "--output-dir",
        default="data/licenses/texas",
        help="Output directory for CSV files"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Texas Dental License Data Fetch")
    print("=" * 60)
    print(f"Source: Texas State Board of Dental Examiners")
    print(f"Output: {args.output_dir}")
    print("=" * 60)

    results = fetch_texas(args.output_dir)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Date: {results['date']}")
    print(f"Total records: {results['total_records']:,}")
    print(f"Files updated: {len([f for f in results['files'] if f['changed']])}")
    print(f"Files unchanged: {len(results['unchanged'])}")
    print(f"Errors: {len(results['errors'])}")

    if results['errors']:
        print(f"\nFailed downloads: {', '.join(results['errors'])}")

    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
