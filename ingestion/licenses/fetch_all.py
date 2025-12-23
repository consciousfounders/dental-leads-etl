#!/usr/bin/env python3
"""
Master orchestration script for fetching dental license data from all automated states.

This script runs all individual state fetch scripts and consolidates the results.

Usage:
    python fetch_all.py
    python fetch_all.py --states TX,WA,CO
    python fetch_all.py --active-only
    python fetch_all.py --parallel
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

# Define available states and their fetch methods
AUTOMATED_STATES = {
    "TX": {
        "name": "Texas",
        "method": "direct_csv",
        "script": None,  # Direct download, no script needed
        "urls": [
            ("dentist", "https://ls.tsbde.texas.gov/lib/csv/Dentist.csv"),
            ("hygienist", "https://ls.tsbde.texas.gov/lib/csv/Hygienist.csv"),
            ("dental_assistant", "https://ls.tsbde.texas.gov/lib/csv/DentalAssistant.csv"),
            ("lab", "https://ls.tsbde.texas.gov/lib/csv/Labs.csv"),
            ("etn", "https://ls.tsbde.texas.gov/lib/csv/ETN.csv"),
        ]
    },
    "WA": {
        "name": "Washington",
        "method": "api_script",
        "script": "fetch_washington.py",
    },
    "CO": {
        "name": "Colorado",
        "method": "api_script",
        "script": "fetch_colorado.py",
    },
    "FL": {
        "name": "Florida",
        "method": "api_script",
        "script": "fetch_florida.py",
    },
}


def download_texas_csv(url: str, output_path: str) -> bool:
    """Download a Texas CSV file directly."""
    import requests

    try:
        print(f"  Downloading {url}...")
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(response.content)

        print(f"  Saved to {output_path}")
        return True
    except Exception as e:
        print(f"  Error downloading {url}: {e}")
        return False


def fetch_texas(output_base: str, active_only: bool = False) -> Dict:
    """Fetch Texas data via direct CSV download."""
    print("\n" + "=" * 60)
    print("Fetching Texas (Direct CSV)")
    print("=" * 60)

    state_dir = os.path.join(output_base, "texas", "raw")
    os.makedirs(state_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    results = {"state": "TX", "files": [], "errors": []}

    for file_type, url in AUTOMATED_STATES["TX"]["urls"]:
        output_path = os.path.join(state_dir, f"tx_{file_type}_{timestamp}.csv")
        success = download_texas_csv(url, output_path)

        if success:
            results["files"].append(output_path)
        else:
            results["errors"].append(f"Failed to download {file_type}")

    return results


def fetch_via_script(state_code: str, output_base: str, active_only: bool = False) -> Dict:
    """Run a state fetch script."""
    state_info = AUTOMATED_STATES[state_code]
    script_name = state_info["script"]

    print("\n" + "=" * 60)
    print(f"Fetching {state_info['name']} ({state_code})")
    print("=" * 60)

    script_path = os.path.join(os.path.dirname(__file__), script_name)
    state_dir = os.path.join(output_base, state_info["name"].lower(), "raw")

    cmd = [sys.executable, script_path, "--output-dir", state_dir]
    if active_only:
        cmd.append("--active-only")

    results = {"state": state_code, "files": [], "errors": []}

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
        )

        if result.returncode == 0:
            print(result.stdout)
            # Find output files
            if os.path.exists(state_dir):
                for f in os.listdir(state_dir):
                    if f.endswith(".csv"):
                        results["files"].append(os.path.join(state_dir, f))
        else:
            print(f"Error: {result.stderr}")
            results["errors"].append(result.stderr)

    except subprocess.TimeoutExpired:
        results["errors"].append("Script timed out after 10 minutes")
    except Exception as e:
        results["errors"].append(str(e))

    return results


def fetch_state(state_code: str, output_base: str, active_only: bool = False) -> Dict:
    """Fetch data for a single state."""
    if state_code not in AUTOMATED_STATES:
        return {"state": state_code, "files": [], "errors": [f"Unknown state: {state_code}"]}

    state_info = AUTOMATED_STATES[state_code]

    if state_info["method"] == "direct_csv":
        return fetch_texas(output_base, active_only)
    else:
        return fetch_via_script(state_code, output_base, active_only)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch dental license data from all automated states"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Base output directory"
    )
    parser.add_argument(
        "--states",
        default=None,
        help="Comma-separated list of state codes (default: all)"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only fetch active licenses"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run state fetches in parallel"
    )

    args = parser.parse_args()

    # Determine which states to fetch
    if args.states:
        states = [s.strip().upper() for s in args.states.split(",")]
    else:
        states = list(AUTOMATED_STATES.keys())

    print("=" * 60)
    print("Dental License Data - Multi-State Fetch")
    print("=" * 60)
    print(f"States: {', '.join(states)}")
    print(f"Output dir: {args.output_dir}")
    print(f"Active only: {args.active_only}")
    print(f"Parallel: {args.parallel}")
    print("=" * 60)

    all_results = []

    if args.parallel:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(fetch_state, state, args.output_dir, args.active_only): state
                for state in states
            }
            for future in as_completed(futures):
                state = futures[future]
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    all_results.append({
                        "state": state,
                        "files": [],
                        "errors": [str(e)]
                    })
    else:
        for state in states:
            result = fetch_state(state, args.output_dir, args.active_only)
            all_results.append(result)

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total_files = 0
    total_errors = 0

    for result in all_results:
        state = result["state"]
        files = result["files"]
        errors = result["errors"]

        status = "✓" if not errors else "✗"
        print(f"\n{status} {state}: {len(files)} files")

        if files:
            for f in files:
                # Get file size
                size = os.path.getsize(f) if os.path.exists(f) else 0
                size_mb = size / (1024 * 1024)
                print(f"    - {os.path.basename(f)} ({size_mb:.1f} MB)")
            total_files += len(files)

        if errors:
            for e in errors:
                print(f"    ERROR: {e}")
            total_errors += len(errors)

    print("\n" + "-" * 60)
    print(f"Total: {total_files} files, {total_errors} errors")
    print("=" * 60)

    # Return exit code
    sys.exit(0 if total_errors == 0 else 1)


if __name__ == "__main__":
    main()
