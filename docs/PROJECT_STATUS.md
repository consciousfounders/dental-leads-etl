# Dental License Data Pipeline - Project Status

**Last Updated:** 2025-12-23

## Overview

Data pipeline for dental professional license data with enrichment, NPI matching, and export to downstream systems (CRM, email, mail).

## Directory Structure

```
data-pipeline/
├── ingestion/licenses/connectors/   # State license fetchers
│   └── fetch_texas.py               # TX dentist/hygienist/assistant data
├── enrichment/
│   └── apollo_enrich.py             # Apollo.io CLI enrichment tool
├── ops/
│   ├── validate_load.py             # Data validation before promotion
│   ├── quarantine_load.py           # Rollback and cascade handling
│   └── export_queue.py              # Export queue management
├── warehouse/
│   ├── ddl/
│   │   ├── data_governance.sql      # Load registry, export queue tables
│   │   └── enrichment_sources.sql   # Apollo, Wiza, scraped data tables
│   └── dbt/models/integration/
│       ├── int_npi_license_matches.sql   # Fuzzy NPI matching
│       ├── int_provider_enrichments.sql  # Enrichment waterfall
│       └── int_provider_golden.sql       # Golden record
├── exports/                         # Output CSVs
│   ├── tx_new_dentists_6mo.csv      # 631 new TX dentists
│   ├── tx_new_hygienists_6mo.csv    # 677 new TX hygienists
│   └── tx_new_assistants_6mo.csv    # 3,650 new TX assistants
└── data/                            # Raw data snapshots
    └── snapshots/YYYY-MM-DD/        # Date-versioned snapshots
```

## Data Sources

### State Licenses (Primary)
- **Texas (TX)**: Dentists, Hygienists, Dental Assistants
  - Source: TSBDE public records
  - Key fields: `LIC_ID` (unique), `LIC_NBR` (can be reused)
  - Active statuses: 20, 46, 70

### NPI Registry
- NPPES API for provider lookup
- Provides: Phone, practice address, taxonomy codes
- Match logic: Name + City/State/ZIP (SOUNDEX for fuzzy)

### Enrichment Sources
| Source | Primary Use | Cost |
|--------|-------------|------|
| Apollo | Email, company, LinkedIn | 1 credit/match |
| Wiza | LinkedIn-based email | Varies |
| Scrapers | Practice websites, reviews | Free |

## Enrichment Waterfall (Priority)

| Field | Priority Order |
|-------|----------------|
| Email | Apollo > Wiza > Scraped |
| Phone | NPI > Apollo > Wiza > Scraped |
| LinkedIn | Wiza > Apollo |
| Website | Scraped > Apollo > Wiza |
| Company | Apollo > Wiza > Scraped > NPI |

## Apollo Enrichment CLI

```bash
# Check budget
python3 enrichment/apollo_enrich.py --usage

# Dry run (no credits)
python3 enrichment/apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --dry-run

# Run with limit
python3 enrichment/apollo_enrich.py --input exports/tx_new_dentists_6mo.csv --max-credits 100

# Full run (auto-stops at 2500/month budget)
python3 enrichment/apollo_enrich.py --input exports/tx_new_dentists_6mo.csv
```

**Budget:** 2500 credits/month tracked in `~/.apollo_usage.json`

**What 1 credit gets you:** Email + name + title + company + LinkedIn + location

## Data Governance

### Load Registry
- Every data load gets a `load_id`
- States: `pending` → `validated` → `promoted` | `quarantined`
- Validation rules per source type in `validate_load.py`

### Quarantine & Rollback
- Bad loads can be quarantined with cascade to exports
- Reversible destinations: GHL (delete contact), Lob (cancel if not sent)
- Non-reversible: Webhooks, Instantly (emails can't be unsent)

### Export Queue
- Tiered approval based on destination cost
- Auto-approve thresholds by match confidence
- Suppression list support

## Golden Record Schema

Key fields in `int_provider_golden`:
- `provider_id` - NPI or state-license ID
- `outreach_readiness` - email_ready | phone_only | mail_only | no_contact
- `is_new_licensee` - Licensed within 180 days
- `enrichment_score` - 0-100 quality score
- `*_source` fields - Track where each field came from

## Current Data (as of 2025-12-23)

**TX New Licensees (6 months):**
- Dentists: 631
- Hygienists: 677
- Dental Assistants: 3,650

## Next Steps

1. Set `APOLLO_API_KEY` and run enrichment on dentist file
2. Connect to Snowflake and deploy DDL
3. Set up dbt project and run models
4. Configure export destinations (GHL, etc.)

## Environment Variables

```bash
export APOLLO_API_KEY='your-key'
export GHL_API_KEY='your-key'        # GoHighLevel
export LOB_API_KEY='your-key'        # Lob mail
export SNOWFLAKE_ACCOUNT='...'
export SNOWFLAKE_USER='...'
export SNOWFLAKE_PASSWORD='...'
```
