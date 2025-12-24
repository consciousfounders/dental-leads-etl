#!/usr/bin/env python3
"""
Apollo.io Enrichment CLI

Enriches dental license records with Apollo.io People API.
Retrieves: email, phone, LinkedIn, company info.

Usage:
    # Single record test
    python apollo_enrich.py --test "John Smith" --city "Houston" --state "TX"

    # Enrich CSV file (email only - 1 credit per match)
    python apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --output exports/tx_dentists_enriched.csv

    # Limit credit spend (stops after N successful matches)
    python apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --max-credits 100

    # Enrich with rate limiting and resume
    python apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --batch-size 50 --delay 1.0

    # Dry run (show what would be sent, no API calls)
    python apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --dry-run --limit 5

Environment:
    APOLLO_API_KEY - Your Apollo.io API key

API Costs:
    - Email lookup: 1 credit per successful match (includes name, company, LinkedIn, location)
    - Phone reveal: +5 credits (use --with-phone flag)
    - No charge for non-matches
    - Check your plan limits at app.apollo.io

Budget Management:
    Default monthly budget: 2500 credits
    Tracks usage in ~/.apollo_usage.json
    Use --budget to set custom limit for this run
    Use --show-usage to see current cycle usage
"""

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import requests


APOLLO_API_URL = "https://api.apollo.io/v1/people/match"
USAGE_FILE = Path.home() / ".apollo_usage.json"
DEFAULT_MONTHLY_BUDGET = 2500


# =============================================================================
# BUDGET TRACKING
# =============================================================================

def get_billing_cycle_key() -> str:
    """Get current billing cycle key (YYYY-MM format)."""
    return datetime.now().strftime("%Y-%m")


def load_usage() -> Dict:
    """Load usage data from file."""
    if USAGE_FILE.exists():
        with open(USAGE_FILE, 'r') as f:
            return json.load(f)
    return {"cycles": {}, "monthly_budget": DEFAULT_MONTHLY_BUDGET}


def save_usage(usage: Dict):
    """Save usage data to file."""
    with open(USAGE_FILE, 'w') as f:
        json.dump(usage, f, indent=2)


def get_cycle_usage() -> int:
    """Get credits used in current billing cycle."""
    usage = load_usage()
    cycle = get_billing_cycle_key()
    return usage.get("cycles", {}).get(cycle, {}).get("credits_used", 0)


def get_remaining_budget() -> int:
    """Get remaining credits for current billing cycle."""
    usage = load_usage()
    budget = usage.get("monthly_budget", DEFAULT_MONTHLY_BUDGET)
    used = get_cycle_usage()
    return max(0, budget - used)


def record_credits(credits: int, source_file: str = ""):
    """Record credits used."""
    usage = load_usage()
    cycle = get_billing_cycle_key()

    if "cycles" not in usage:
        usage["cycles"] = {}
    if cycle not in usage["cycles"]:
        usage["cycles"][cycle] = {"credits_used": 0, "runs": []}

    usage["cycles"][cycle]["credits_used"] += credits
    usage["cycles"][cycle]["runs"].append({
        "timestamp": datetime.now().isoformat(),
        "credits": credits,
        "source": source_file,
    })

    save_usage(usage)


def show_usage():
    """Display current usage stats."""
    usage = load_usage()
    cycle = get_billing_cycle_key()
    budget = usage.get("monthly_budget", DEFAULT_MONTHLY_BUDGET)
    cycle_data = usage.get("cycles", {}).get(cycle, {})
    used = cycle_data.get("credits_used", 0)
    remaining = max(0, budget - used)
    runs = cycle_data.get("runs", [])

    print(f"\n{'='*50}")
    print(f"Apollo Credit Usage - {cycle}")
    print(f"{'='*50}")
    print(f"Monthly budget:  {budget:,} credits")
    print(f"Used this cycle: {used:,} credits")
    print(f"Remaining:       {remaining:,} credits ({remaining/budget*100:.1f}%)")

    if runs:
        print(f"\nRecent runs:")
        for run in runs[-5:]:  # Last 5 runs
            ts = run.get("timestamp", "")[:16].replace("T", " ")
            print(f"  {ts}: {run.get('credits', 0)} credits - {run.get('source', 'unknown')}")

    print()


@dataclass
class EnrichmentResult:
    """Result from Apollo enrichment."""
    # Input keys
    license_id: str
    license_number: str
    first_name: str
    last_name: str
    input_city: str
    input_state: str

    # Apollo results
    apollo_id: Optional[str] = None
    email: Optional[str] = None
    email_status: Optional[str] = None  # verified, guessed, etc.
    phone: Optional[str] = None
    mobile_phone: Optional[str] = None
    linkedin_url: Optional[str] = None

    # Company info
    company_name: Optional[str] = None
    company_domain: Optional[str] = None
    title: Optional[str] = None

    # Address from Apollo
    apollo_city: Optional[str] = None
    apollo_state: Optional[str] = None
    apollo_country: Optional[str] = None

    # Meta
    match_score: Optional[float] = None
    enriched_at: Optional[str] = None
    api_error: Optional[str] = None


def enrich_person(
    first_name: str,
    last_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    organization_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict:
    """
    Call Apollo People Match API.

    Args:
        first_name: Person's first name
        last_name: Person's last name
        city: City for location matching
        state: State for location matching
        organization_name: Company name if known
        api_key: Apollo API key (or uses env var)

    Returns:
        Dict with Apollo response data
    """
    api_key = api_key or os.environ.get('APOLLO_API_KEY')
    if not api_key:
        raise ValueError("APOLLO_API_KEY environment variable not set")

    # Build request payload
    payload = {
        "api_key": api_key,
        "first_name": first_name,
        "last_name": last_name,
        "reveal_personal_emails": True,  # Get personal emails too
    }

    # Add location context if available
    if city:
        payload["city"] = city
    if state:
        payload["state"] = state
    if organization_name:
        payload["organization_name"] = organization_name

    try:
        response = requests.post(
            APOLLO_API_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            return {"error": "Invalid API key"}
        elif response.status_code == 429:
            return {"error": "Rate limited - slow down requests"}
        else:
            return {"error": f"API error: {response.status_code} - {response.text}"}

    except requests.exceptions.Timeout:
        return {"error": "Request timeout"}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}


def parse_apollo_response(response: Dict, input_data: Dict) -> EnrichmentResult:
    """Parse Apollo API response into EnrichmentResult."""
    result = EnrichmentResult(
        license_id=input_data.get('license_id', ''),
        license_number=input_data.get('license_number', ''),
        first_name=input_data.get('first_name', ''),
        last_name=input_data.get('last_name', ''),
        input_city=input_data.get('city', ''),
        input_state=input_data.get('state', ''),
        enriched_at=datetime.now().isoformat(),
    )

    if "error" in response:
        result.api_error = response["error"]
        return result

    person = response.get("person", {})
    if not person:
        result.api_error = "No match found"
        return result

    # Extract contact info
    result.apollo_id = person.get("id")
    result.email = person.get("email")
    result.email_status = person.get("email_status")

    # Phone numbers
    phone_numbers = person.get("phone_numbers", [])
    for phone in phone_numbers:
        if phone.get("type") == "mobile":
            result.mobile_phone = phone.get("sanitized_number")
        else:
            result.phone = phone.get("sanitized_number")

    # LinkedIn
    result.linkedin_url = person.get("linkedin_url")

    # Current employment
    employment = person.get("employment_history", [])
    if employment:
        current = employment[0]  # First is usually current
        result.company_name = current.get("organization_name")
        result.title = current.get("title")

    # Organization info
    org = person.get("organization", {})
    if org:
        result.company_name = result.company_name or org.get("name")
        result.company_domain = org.get("primary_domain")

    # Location
    result.apollo_city = person.get("city")
    result.apollo_state = person.get("state")
    result.apollo_country = person.get("country")

    return result


def enrich_csv(
    input_file: str,
    output_file: str,
    batch_size: int = 100,
    delay: float = 0.5,
    limit: Optional[int] = None,
    max_credits: Optional[int] = None,
    dry_run: bool = False,
    resume: bool = True,
) -> Dict:
    """
    Enrich a CSV file with Apollo data.

    Args:
        input_file: Path to input CSV
        output_file: Path to output CSV
        batch_size: Records per batch (for progress reporting)
        delay: Seconds between API calls
        limit: Max records to process
        max_credits: Stop after this many successful matches (credits used)
        dry_run: If True, just show what would be sent
        resume: If True, skip already-processed records

    Returns:
        Dict with statistics
    """
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    # Load existing results for resume
    processed_ids = set()
    existing_results = []
    if resume and output_path.exists():
        with open(output_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_results.append(row)
                processed_ids.add(row.get('license_id', ''))
        print(f"Resuming: {len(processed_ids)} records already processed")

    # Read input
    with open(input_path, 'r') as f:
        reader = csv.DictReader(f)
        input_rows = list(reader)

    print(f"Input file: {len(input_rows)} records")

    # Filter to unprocessed
    to_process = [
        row for row in input_rows
        if row.get('LIC_ID', '') not in processed_ids
    ]

    if limit:
        to_process = to_process[:limit]

    print(f"To process: {len(to_process)} records")

    if dry_run:
        print("\n=== DRY RUN - Sample payloads ===\n")
        for i, row in enumerate(to_process[:5]):
            print(f"Record {i+1}:")
            print(f"  Name: {row.get('FIRST_NME', '')} {row.get('LAST_NME', '')}")
            print(f"  City: {row.get('CITY', '')}, State: {row.get('STATE', '')}")
            print(f"  License: {row.get('LIC_NBR', '')} ({row.get('LIC_ID', '')})")
            print()
        return {"dry_run": True, "would_process": len(to_process)}

    # Check API key
    api_key = os.environ.get('APOLLO_API_KEY')
    if not api_key:
        print("ERROR: APOLLO_API_KEY environment variable not set")
        print("\nTo set it:")
        print("  export APOLLO_API_KEY='your-api-key-here'")
        sys.exit(1)

    # Check budget before starting
    remaining_budget = get_remaining_budget()
    effective_limit = max_credits or remaining_budget

    print(f"\nBudget status:")
    print(f"  Remaining this cycle: {remaining_budget:,} credits")
    print(f"  This run limit: {effective_limit:,} credits")

    if remaining_budget == 0:
        print("\n[STOP] No credits remaining in budget. Use --max-credits to override.")
        return {"error": "no_budget"}

    if remaining_budget < 50:
        print(f"\n[WARN] Low budget: only {remaining_budget} credits remaining")

    # Process records
    results = existing_results.copy()
    stats = {
        "processed": 0,
        "matched": 0,
        "emails_found": 0,
        "phones_found": 0,
        "errors": 0,
    }

    output_fields = [
        'license_id', 'license_number', 'first_name', 'last_name',
        'input_city', 'input_state', 'apollo_id', 'email', 'email_status',
        'phone', 'mobile_phone', 'linkedin_url', 'company_name',
        'company_domain', 'title', 'apollo_city', 'apollo_state',
        'apollo_country', 'match_score', 'enriched_at', 'api_error'
    ]

    print(f"\nStarting enrichment (delay: {delay}s between calls)...")
    start_time = datetime.now()

    for i, row in enumerate(to_process):
        # Check credit limit before making API call
        if stats["matched"] >= effective_limit:
            print(f"\n[STOP] Credit limit reached: {stats['matched']} credits used")
            break
        # Map input fields
        input_data = {
            'license_id': row.get('LIC_ID', ''),
            'license_number': row.get('LIC_NBR', ''),
            'first_name': row.get('FIRST_NME', ''),
            'last_name': row.get('LAST_NME', ''),
            'city': row.get('CITY', ''),
            'state': row.get('STATE', ''),
        }

        # Call Apollo API
        response = enrich_person(
            first_name=input_data['first_name'],
            last_name=input_data['last_name'],
            city=input_data['city'],
            state=input_data['state'],
            api_key=api_key,
        )

        # Parse response
        result = parse_apollo_response(response, input_data)
        results.append(asdict(result))

        # Update stats
        stats["processed"] += 1
        if result.apollo_id:
            stats["matched"] += 1
        if result.email:
            stats["emails_found"] += 1
        if result.phone or result.mobile_phone:
            stats["phones_found"] += 1
        if result.api_error:
            stats["errors"] += 1

        # Progress reporting
        if (i + 1) % batch_size == 0 or (i + 1) == len(to_process):
            elapsed = (datetime.now() - start_time).seconds
            rate = stats["processed"] / max(elapsed, 1)
            print(f"  Processed: {stats['processed']}/{len(to_process)} "
                  f"| Matched: {stats['matched']} "
                  f"| Emails: {stats['emails_found']} "
                  f"| Rate: {rate:.1f}/sec")

            # Write intermediate results
            with open(output_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=output_fields)
                writer.writeheader()
                writer.writerows(results)

        # Rate limiting
        if i < len(to_process) - 1:
            time.sleep(delay)

    # Final write
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(results)

    # Record credits used
    if stats["matched"] > 0:
        record_credits(stats["matched"], str(input_path.name))

    new_remaining = get_remaining_budget()

    print(f"\nEnrichment complete!")
    print(f"  Output: {output_path}")
    print(f"  Total processed: {stats['processed']}")
    print(f"  Matches: {stats['matched']} ({stats['matched']/max(stats['processed'],1)*100:.1f}%)")
    print(f"  Emails found: {stats['emails_found']} ({stats['emails_found']/max(stats['processed'],1)*100:.1f}%)")
    print(f"  Phones found: {stats['phones_found']} ({stats['phones_found']/max(stats['processed'],1)*100:.1f}%)")
    print(f"\n  Credits used: {stats['matched']}")
    print(f"  Remaining budget: {new_remaining:,} credits")

    return stats


def test_single(name: str, city: str, state: str):
    """Test enrichment for a single person."""
    parts = name.split(' ', 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ''

    print(f"Testing: {first_name} {last_name}, {city}, {state}")
    print()

    api_key = os.environ.get('APOLLO_API_KEY')
    if not api_key:
        print("ERROR: APOLLO_API_KEY not set")
        print("  export APOLLO_API_KEY='your-key'")
        return

    response = enrich_person(first_name, last_name, city, state, api_key=api_key)

    if "error" in response:
        print(f"Error: {response['error']}")
        return

    person = response.get("person", {})
    if not person:
        print("No match found")
        return

    print("=== Apollo Match ===")
    print(f"  Name: {person.get('first_name')} {person.get('last_name')}")
    print(f"  Email: {person.get('email')} ({person.get('email_status', 'unknown')})")

    phones = person.get('phone_numbers', [])
    for p in phones:
        print(f"  Phone ({p.get('type', 'unknown')}): {p.get('sanitized_number')}")

    print(f"  LinkedIn: {person.get('linkedin_url')}")
    print(f"  Title: {person.get('title')}")

    org = person.get('organization', {})
    if org:
        print(f"  Company: {org.get('name')}")
        print(f"  Domain: {org.get('primary_domain')}")

    print(f"  Location: {person.get('city')}, {person.get('state')}")
    print()
    print("Raw response:")
    print(json.dumps(response, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Enrich dental records with Apollo.io data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check current credit usage
  python apollo_enrich.py --usage

  # Test single lookup (uses 1 credit if match found)
  python apollo_enrich.py --test "John Smith" --city Houston --state TX

  # Dry run to preview (no credits used)
  python apollo_enrich.py --input data.csv --dry-run --limit 5

  # Full enrichment (auto-stops at budget limit)
  python apollo_enrich.py --input data.csv --output enriched.csv

  # Limit this run to 100 credits
  python apollo_enrich.py --input data.csv --max-credits 100

  # Slow rate for large batches
  python apollo_enrich.py --input data.csv --output enriched.csv --delay 2.0
        """
    )

    parser.add_argument('--input', '-i', help='Input CSV file')
    parser.add_argument('--output', '-o', help='Output CSV file')
    parser.add_argument('--test', help='Test with single name (e.g., "John Smith")')
    parser.add_argument('--city', help='City for test lookup')
    parser.add_argument('--state', help='State for test lookup')
    parser.add_argument('--batch-size', type=int, default=50, help='Progress report interval')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between API calls (seconds)')
    parser.add_argument('--limit', type=int, help='Max records to process (attempts)')
    parser.add_argument('--max-credits', type=int, help='Override: stop after N credits (ignores budget)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without calling API')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh, ignore existing output')
    parser.add_argument('--usage', action='store_true', help='Show current credit usage and exit')

    args = parser.parse_args()

    if args.usage:
        show_usage()
    elif args.test:
        test_single(args.test, args.city or '', args.state or '')
    elif args.input:
        if not args.output:
            # Default output name
            input_path = Path(args.input)
            args.output = str(input_path.parent / f"{input_path.stem}_enriched.csv")

        enrich_csv(
            input_file=args.input,
            output_file=args.output,
            batch_size=args.batch_size,
            delay=args.delay,
            limit=args.limit,
            max_credits=args.max_credits,
            dry_run=args.dry_run,
            resume=not args.no_resume,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
