# Dental License Data Pipeline Architecture

## Overview

This pipeline ingests dental professional license data from all 50 US states + territories, detects changes between snapshots, and generates events that trigger downstream marketing actions.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   State Board   │     │   State Board   │     │   State Board   │
│   CSV / API     │     │   CSV / API     │     │   CSV / API     │
│     (Texas)     │     │  (California)   │     │   (50 states)   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                     RAW LAYER (Snowflake)                       │
│  raw_texas.dentist  │  raw_california.dentist  │  raw_*.dentist │
│  raw_texas.hygienist│  raw_california.hygienist│  ...           │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STAGING LAYER (dbt views)                     │
│  • Standardize column names across states                       │
│  • Parse dates, normalize status codes                          │
│  • Add source tracking (state_code, professional_type)          │
│  stg_tx_dentist │ stg_tx_hygienist │ stg_ca_dentist │ ...       │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│               INTERMEDIATE LAYER (dbt views)                    │
│  • Union all states/professional types                          │
│  • Create unified schema                                        │
│  • Generate surrogate keys                                      │
│  int_dental_professionals_unioned                               │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 SNAPSHOT LAYER (dbt snapshots)                  │
│  • SCD Type 2 history tracking                                  │
│  • Tracks changes to: status, address, certifications           │
│  • Enables point-in-time analysis                               │
│  snp_dental_professionals                                       │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MARTS LAYER (dbt tables)                     │
│                                                                 │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │ dim_dental_         │     │ fct_license_events  │           │
│  │ professionals       │     │                     │           │
│  │ (current state)     │     │ • NEW_LICENSE       │           │
│  │                     │     │ • STATUS_LAPSED     │           │
│  │ • Segmentation      │     │ • STATUS_REINSTATED │           │
│  │ • Marketing scores  │     │ • ADDRESS_CHANGE    │           │
│  │ • Certification     │     │ • NEW_CERTIFICATION │           │
│  │   summaries         │     │ • EXPIRATION_NEAR   │           │
│  └─────────────────────┘     └─────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DOWNSTREAM SYSTEMS                            │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │   GHL    │  │ Instantly│  │   Ad     │  │  Direct  │        │
│  │   CRM    │  │  (Cold   │  │ Platforms│  │   Mail   │        │
│  │          │  │ Outreach)│  │          │  │          │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Data Model

### Primary Key Strategy

Each professional is identified by a composite key:
- `state_code` (TX, CA, NY, etc.)
- `professional_type` (dentist, hygienist, dental_assistant, dental_lab)
- `license_number` (state-specific)

This generates a `professional_id` surrogate key using dbt_utils.generate_surrogate_key().

### Schema Design Principles

1. **State-Agnostic Core**: Common fields work across all states
2. **Type-Specific Extensions**: Certifications vary by professional type
3. **Flexible Certification Tracking**: Boolean flags for each cert type
4. **Event-Driven Architecture**: Changes generate actionable events

## Event Types

| Event Type | Trigger | Marketing Action | Priority |
|------------|---------|------------------|----------|
| NEW_LICENSE | New record in source | Onboarding campaign | HIGH |
| STATUS_LAPSED | Active → Cancelled/Expired | Win-back or suppress | MEDIUM |
| STATUS_REINSTATED | Lapsed → Active | Re-engagement | HIGH |
| STATUS_DECEASED | Any → Deceased | Remove from all lists | CRITICAL |
| ADDRESS_CHANGE | Address fields changed | Update CRM, reroute | LOW |
| TERRITORY_CHANGE | County changed | Territory reassignment | MEDIUM |
| NEW_CERTIFICATION | Cert flag false → true | Upsell relevant products | HIGH |
| EXPIRATION_APPROACHING | <90 days to expiration | Renewal reminder | MEDIUM |

## Adding a New State

### Step 1: Create Source Configuration
Add to `models/staging/_sources.yml`:
```yaml
- name: california_raw
  database: dental_data
  schema: raw_california
  tables:
    - name: dentist
    - name: hygienist
```

### Step 2: Create Staging Models
Copy and adapt `stg_tx_dentist.sql` to `stg_ca_dentist.sql`:
- Map state-specific column names to standard schema
- Handle state-specific date formats
- Map state-specific status codes

### Step 3: Add to Union Model
Update `int_dental_professionals_unioned.sql` to include new state.

### Step 4: Run Snapshot
The snapshot will automatically capture the new records.

## NPI Integration

The `entity_nbr` field in Texas data may link to NPI records. Future integration:

```sql
-- Potential join to your existing NPI data
select
    dp.*,
    npi.npi_number,
    npi.provider_organization_name
from {{ ref('dim_dental_professionals') }} dp
left join {{ source('npi', 'providers') }} npi
    on dp.entity_number = npi.entity_number
    and dp.state_code = npi.provider_state
```

## Deployment

### Daily Run Schedule (Recommended)

```bash
# 1. Fetch new data from all state sources
python scripts/fetch_state_data.py --all-states

# 2. Load to Snowflake raw layer
snowsql -f scripts/load_raw.sql

# 3. Run dbt pipeline
dbt deps
dbt snapshot  # Capture changes
dbt run       # Transform data
dbt test      # Validate

# 4. Export events to downstream
python scripts/export_events.py --since yesterday
```

### Snowflake Warehouse Sizing

| Stage | Compute | Notes |
|-------|---------|-------|
| Raw Load | XS | Bulk CSV load |
| Staging Views | XS | On-demand |
| Snapshot | S | SCD2 comparisons |
| Marts | S | Full refresh |
| Event Export | XS | Incremental |

## File Structure

```
dental-license-pipeline/
├── dbt_project.yml
├── packages.yml
├── profiles.yml (not in repo)
├── data/
│   ├── texas/
│   │   └── raw/
│   │       ├── Dentist.csv
│   │       ├── Hygienist.csv
│   │       ├── DentalAssistant.csv
│   │       ├── Labs.csv
│   │       └── ETN.csv
│   └── california/
│       └── raw/
├── docs/
│   ├── architecture.md
│   └── texas_schema_analysis.md
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── stg_tx_dentist.sql
│   │   ├── stg_tx_hygienist.sql
│   │   ├── stg_tx_dental_assistant.sql
│   │   └── stg_tx_lab.sql
│   ├── intermediate/
│   │   └── int_dental_professionals_unioned.sql
│   ├── snapshots/
│   │   └── snp_dental_professionals.sql
│   └── marts/
│       ├── dim_dental_professionals.sql
│       └── fct_license_events.sql
├── seeds/
│   ├── state_board_urls.csv
│   ├── specialty_codes.csv
│   └── status_codes.csv
├── scripts/
│   ├── fetch_state_data.py
│   ├── load_raw.sql
│   └── export_events.py
└── tests/
    └── generic/
```
