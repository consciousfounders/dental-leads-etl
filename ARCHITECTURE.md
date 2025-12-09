# Dental Leads Platform - Architecture

> **Purpose**: Sales intelligence platform targeting dental offices for OnPharma product sales using omni-channel outbound marketing.

---

## Table of Contents
- [System Overview](#system-overview)
- [Data Flow](#data-flow)
- [Infrastructure](#infrastructure)
- [Schema Design](#schema-design)
- [Integrations](#integrations)
- [ML / AI Layer](#ml--ai-layer)
- [Key Architectural Decisions](#key-architectural-decisions)
- [Current Status](#current-status)
- [Roadmap / Next Steps](#roadmap--next-steps)

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        DENTAL LEADS PLATFORM                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│                         ┌─────────────────────────────┐                             │
│                         │      ORCHESTRATION LAYER    │                             │
│                         │         (GCP VM)            │                             │
│                         │                             │                             │
│                         │  • Enrichment APIs          │                             │
│                         │  • De-anonymization svc     │                             │
│                         │  • Webhook receivers        │                             │
│                         │  • Scheduled jobs           │                             │
│                         │                             │                             │
│                         │  ** Warehouse-agnostic **   │                             │
│                         └──────────────┬──────────────┘                             │
│                                        │                                            │
│                                        ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                    DATA WAREHOUSE (Snowflake / BigQuery)                      │  │
│  │                                                                               │  │
│  │   RAW ──────▶ CLEAN ──────▶ ENRICHED ──────▶ SEGMENTED ──────▶ ACTIVATED    │  │
│  │                                                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                        │                                            │
│                                        ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                         ACTIVATION LAYER                                      │  │
│  │                                                                               │  │
│  │   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐     │  │
│  │   │ Cold Email  │   │ Display Ads │   │   Website   │   │     CRM     │     │  │
│  │   │ (Instantly/ │   │ (Google/    │   │  (Landing   │   │    (GHL)    │     │  │
│  │   │  Smartlead) │   │  Meta/TTD)  │   │   Pages)    │   │             │     │  │
│  │   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘     │  │
│  │          │                 │                 │                 │            │  │
│  └──────────┼─────────────────┼─────────────────┼─────────────────┼────────────┘  │
│             │                 │                 │                 │               │
│             ▼                 ▼                 ▼                 ▼               │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                      FEEDBACK / ATTRIBUTION LAYER                             │  │
│  │                                                                               │  │
│  │   Events: email_open, email_click, ad_impression, ad_click, page_view,       │  │
│  │           form_submit, demo_booked, deal_won                                  │  │
│  │                                                                               │  │
│  │   Attribution: channel, source, campaign_id, creative_id, keyword            │  │
│  │                                                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                        │                                            │
│                                        ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                         ML LAYER (Vertex AI)                                  │  │
│  │                                                                               │  │
│  │   • Lead Scoring (0-100)              • Best Channel Predictor               │  │
│  │   • Conversion Probability            • Optimal Send Time                    │  │
│  │   • Content Recommender                                                       │  │
│  │                                                                               │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Source: CMS NPPES (National Provider Identifier)
- **URL**: https://npiregistry.cms.hhs.gov/
- **Update Frequency**: Weekly (every Sunday)
- **Current Process**: Manual download → upload to VM → unzip → load to Snowflake
- **Target**: Automate with cron job on VM

### Medallion Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│     RAW     │───▶│    CLEAN    │───▶│  ENRICHED   │───▶│  SEGMENTED  │
│             │    │             │    │             │    │             │
│ NPI_PROVIDER│    │ PROVIDERS_  │    │ LEADS_      │    │ LEADS_      │
│ (8M+ rows)  │    │ VALIDATED   │    │ MASTER      │    │ ACTIVATED   │
│             │    │ (~200K)     │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### RAW → CLEAN Transformations
| Transformation | Method | Notes |
|----------------|--------|-------|
| Remove inactive | Filter `NPI Deactivation Date IS NULL` | NPPES field |
| Dental only | Filter by taxonomy codes | ~15 dental taxonomy codes (122300000X, 1223G0001X, etc.) |
| Deduplicate | Match on NPI (unique) | NPI should be unique |
| Address verification | Addy API / SmartyStreets | Normalize + confidence score |
| Phone validation | Twilio Lookup API | Validity + line type + carrier |

### CLEAN → ENRICHED Transformations
| Enrichment | Provider | Priority |
|------------|----------|----------|
| Email | Wiza | 1 (cheapest) |
| Person + Company | Apollo | 2 |
| Multi-source waterfall | Clay | 3 (most comprehensive) |

### ENRICHED → SEGMENTED
- ICP (Ideal Customer Profile) scoring
- Geographic segmentation
- Practice size estimation
- Specialty filtering

---

## Infrastructure

### GCP Components

| Component | Purpose | Details |
|-----------|---------|---------|
| **GCS Bucket** | Data lake | `gs://dl-ingestion-lake/npi/` |
| **GCP VM** | Orchestration hub | Runs ETL, webhooks, enrichment APIs |
| **Vertex AI** | ML models | Lead scoring, predictions |

### VM Role (Orchestration Hub)

The VM is intentionally **warehouse-agnostic** to allow switching between Snowflake and BigQuery:

```
                    ┌─────────────────────────────┐
                    │         GCP VM              │
                    │    (Orchestration Hub)      │
                    ├─────────────────────────────┤
                    │                             │
   Enrichment ─────▶│  Wiza, Apollo, Clay APIs   │
                    │                             │
   Attribution ────▶│  De-anon service webhooks  │
                    │  (RB2B, Leadfeeder, etc.)  │
                    │                             │
   Email Events ───▶│  Email platform webhooks   │
                    │                             │
   CRM Events ─────▶│  GHL webhooks              │
                    │                             │
                    ├─────────────────────────────┤
                    │         WRITES TO           │
                    │   ┌─────────┐ ┌─────────┐  │
                    │   │Snowflake│ │ BigQuery│  │
                    │   └─────────┘ └─────────┘  │
                    │      (swap anytime)        │
                    └─────────────────────────────┘
```

### Snowflake Setup

| Component | Status | Details |
|-----------|--------|---------|
| Database | `DENTAL_LEADS` | Main database |
| Schema | `RAW` | Raw ingestion layer |
| Stage | `@npi_stage` | **TODO: Verify configured** |
| GCS Integration | **TODO: Verify** | Service account for GCS access |

**Verification queries to run:**
```sql
-- Check if stage exists
SHOW STAGES LIKE 'npi_stage' IN SCHEMA RAW;

-- Check if GCS integration exists  
SHOW STORAGE INTEGRATIONS;

-- Check what's in the stage (if it exists)
LIST @npi_stage;
```

---

## Schema Design

### Events Table (Append-Only Attribution)

```sql
CREATE TABLE EVENTS.LEAD_TOUCHPOINTS (
    event_id STRING,
    lead_id STRING,              -- Links to NPI/provider_id
    event_timestamp TIMESTAMP,
    
    -- Attribution
    channel STRING,              -- email, display, organic, direct, referral
    source STRING,               -- instantly, google_ads, meta, website
    campaign_id STRING,
    creative_id STRING,
    ad_group STRING,
    keyword STRING,              -- for paid search
    
    -- Event details
    event_type STRING,           -- email_sent, email_open, email_click, 
                                 -- ad_impression, ad_click, page_view,
                                 -- form_submit, demo_booked, deal_won
    
    -- Metadata (flexible JSON for channel-specific data)
    event_metadata VARIANT,      -- {subject_line, link_clicked, page_url, etc}
    
    -- De-anonymization
    identified_via STRING,       -- clearbit, rb2b, form_fill, email_click
    confidence_score FLOAT,
    
    -- Housekeeping
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Leads Master Table (Current State)

```sql
CREATE TABLE ENRICHED.LEADS_MASTER (
    lead_id STRING PRIMARY KEY,
    npi STRING,
    
    -- From NPI/NPPES
    provider_name STRING,
    practice_name STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,
    taxonomy_code STRING,
    
    -- From enrichment (Wiza/Apollo/Clay)
    email STRING,
    phone STRING,
    linkedin_url STRING,
    decision_maker_name STRING,
    estimated_practice_size STRING,
    tech_stack VARIANT,
    
    -- Engagement metrics (rolled up from events)
    total_emails_sent INT,
    total_opens INT,
    total_clicks INT,
    total_page_views INT,
    last_engaged_at TIMESTAMP,
    
    -- ML outputs
    lead_score FLOAT,
    predicted_conversion_prob FLOAT,
    recommended_channel STRING,
    
    -- Status
    current_stage STRING,        -- new, engaged, qualified, opportunity, customer
    assigned_to STRING,
    
    updated_at TIMESTAMP
);
```

---

## Integrations

### CRM: Go High Level (GHL)

**Flow:**
```
Snowflake ENRICHED.LEADS_MASTER 
    → Export to GCS (or direct API call from VM)
    → GHL API: Create/Update Contact
    → GHL triggers campaigns
    → Webhook back to VM on engagement events
    → VM writes to Snowflake (feedback loop)
```

### De-Anonymization Services (Website Visitor Identification)

| Service | How it Works | Pricing | Notes |
|---------|--------------|---------|-------|
| **RB2B** | IP → Person (B2B) | $$ | Good for dental offices |
| **Leadfeeder** | IP → Company + behavior | $$ | Can match to practice |
| **Clearbit Reveal** | IP → Company | $$$ | Enterprise-focused |
| **6sense** | Intent data + reveal | $$$$ | Enterprise |
| **Snitcher** | IP → Company | $ | Budget option |

**Recommended**: RB2B or Leadfeeder for dental - can resolve to practice and match back to NPI data.

### Enrichment APIs

| Provider | Data | Use Case | Priority |
|----------|------|----------|----------|
| **Wiza** | Email | Email enrichment | 1 (cheapest) |
| **Apollo** | Person + Company | Full profile | 2 |
| **Clay** | Multi-source waterfall | Comprehensive | 3 |

### Validation APIs

| Provider | Data | Use Case |
|----------|------|----------|
| **Addy** | Address | USPS normalization + confidence |
| **Twilio Lookup** | Phone | Validity + line type + carrier |
| **NeverBounce/ZeroBounce** | Email | Deliverability (future) |

---

## ML / AI Layer

### Planned Model: Vertex AI

### Feature Sources

| Feature Category | Source | Examples |
|------------------|--------|----------|
| **Firmographic** | NPI + Enrichment | Practice size, location, specialty, years in practice |
| **Engagement** | Events table | Opens, clicks, page views, time on site |
| **Behavioral** | Events table | Engagement velocity, channel affinity, content resonance |
| **Temporal** | Events table | Time-of-day patterns, day-of-week patterns |

### Model Outputs

| Model | Output | Use Case |
|-------|--------|----------|
| **Lead Score** | 0-100 | Prioritization |
| **Conversion Probability** | 0-1 | Forecasting |
| **Best Channel Predictor** | email/display/phone | Channel optimization |
| **Optimal Send Time** | datetime | Timing optimization |
| **Content Recommender** | creative_id | Personalization |

### AI Content Generation

Use warehouse data to generate:
- Cold email copy (personalized to practice)
- Display ad creative variants
- Landing page content
- Offer messaging

---

## Key Architectural Decisions

### 1. Warehouse-Agnostic Design
**Decision**: Keep enrichment and integrations on VM, not in Snowflake native features.

**Rationale**: 
- May switch to BigQuery or another warehouse
- Don't want connections locked into Snowflake ecosystem
- VM as integration hub provides flexibility

**Implementation**: `utils/db_adapter.py` abstracts warehouse choice via `WAREHOUSE_ENGINE` env var.

### 2. Append-Only Events Table
**Decision**: Store all touchpoints as immutable events, roll up to lead state.

**Rationale**:
- Full attribution history preserved
- ML can learn from sequences
- Debugging/auditing capability
- No data loss from overwrites

### 3. Multi-Channel Attribution
**Decision**: Track channel, source, campaign, creative, keyword for every touchpoint.

**Rationale**:
- Understand which channels drive conversions
- Optimize spend across channels
- A/B test creative at scale
- Feed ML models with rich signal

---

## Current Status

### Completed ✅
- [x] GCS client (`utils/gcs.py`)
- [x] Snowflake client with RSA auth (`utils/snowflake_client.py`)
- [x] BigQuery client (`utils/bigquery_client.py`)
- [x] DB adapter for warehouse switching (`utils/db_adapter.py`)
- [x] NPI ingestion pipeline (`etl/npi_ingestion.py`)
- [x] Repo connected to GitHub (`consciousfounders/dental-leads-etl`)

### Stubbed / In Progress 🟡
- [ ] Validation pipeline - Addy + Twilio APIs (`etl/validation_pipeline.py`)
- [ ] Enrichment pipeline - Wiza/Apollo/Clay (`etl/enrichment_pipeline.py`)
- [ ] VM sync script (`scripts/sync_to_vm.sh`)

### Not Started ❌
- [ ] Snowflake stage verification
- [ ] RAW → CLEAN transformation SQL
- [ ] Events table schema creation
- [ ] Webhook receiver on VM
- [ ] GHL integration
- [ ] De-anonymization service integration
- [ ] ML model development

---

## Roadmap / Next Steps

### 🎯 NEXT SESSION (Priority)
1. **Verify Snowflake setup** - stage, GCS integration, tables
2. **Build RAW → CLEAN SQL** - filter to ~200K active dentists
3. **Initial Wiza enrichment test** - 100 sample leads
4. **Scale Wiza enrichment** - 25K leads batch

### Phase 1: Foundation
1. ~~Verify Snowflake stage + GCS integration~~ → NEXT SESSION
2. ~~Automate NPI download (cron on VM)~~ → SKIPPED (manual is fine for now)
3. ~~Build RAW → CLEAN transformation SQL (get to ~200K dentists)~~ → NEXT SESSION
4. Create events + leads_master tables

### Phase 2: Enrichment & Validation
5. Implement Addy API integration (address validation)
6. Implement Twilio API integration (phone validation)
7. Implement enrichment waterfall (Wiza → Apollo → Clay)

### Phase 3: Activation
8. Build GHL integration (export leads, receive webhooks)
9. Set up webhook receiver on VM
10. Integrate de-anonymization service (RB2B or Leadfeeder)

### Phase 4: Intelligence
11. Build ML feature pipeline
12. Train lead scoring model in Vertex AI
13. Integrate AI content generation

---

## Repository Structure

```
dental-leads-etl/
├── config/
│   ├── config.toml           # Non-sensitive configuration
│   └── secrets_example.toml  # Template for secrets
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yaml
├── etl/
│   ├── npi_ingestion.py      # Load NPI from GCS to Snowflake
│   ├── validation_pipeline.py # Address + phone validation
│   └── enrichment_pipeline.py # Wiza/Apollo/Clay waterfall
├── notebooks/
│   └── exploration.ipynb
├── scripts/
│   ├── run_etl_local.sh
│   ├── setup_secrets.sh
│   └── sync_to_vm.sh
├── sql/
│   └── raw/
│       ├── load_npi.sql
│       ├── load_npi.snowflake.sql
│       └── load_npi.bigquery.sql
├── utils/
│   ├── audit_logger.py
│   ├── bigquery_client.py
│   ├── db_adapter.py
│   ├── gcs.py
│   ├── logging_utils.py
│   ├── secrets_manager.py
│   ├── snowflake_client.py
│   └── snowflake.py
├── validators/
│   ├── __init__.py
│   ├── address_validator.py
│   ├── base.py
│   └── phone_validator.py
├── ARCHITECTURE.md           # This file
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Contact / GitHub

- **Repository**: https://github.com/consciousfounders/dental-leads-etl
- **Owner**: consciousfounders
