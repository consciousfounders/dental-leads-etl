# Data Pipeline Platform

A comprehensive data pipeline and marketing intelligence platform for healthcare provider leads. Consolidates data from multiple sources, transforms it through a medallion architecture (RAW → CLEAN → ENRICHED → SEGMENTED), and activates it through CRM and advertising channels with ML-powered personalization.

## 🏥 Provider Databases

| Schema | Providers | Market |
|--------|-----------|--------|
| `DENTAL` | 230K | Dentists, dental practices |
| `MENTAL_HEALTH` | 1.4M | Therapists, psychologists, psychiatrists, facilities |

## 🏗️ Architecture

```
Snowflake: HEALTHCARE_LEADS
├── RAW            ← NPI source data (9.2M records)
├── DENTAL         ← Dentist-specific transformations
├── MENTAL_HEALTH  ← Mental health provider transformations
└── ENRICHED       ← Wiza/Apollo contact data
```

## 🚀 Quick Start

### 1. Fetch credentials from 1Password
```bash
./scripts/fetch_snowflake_creds.sh
```

### 2. Run the dashboard locally
```bash
streamlit run dashboards/client_dashboard.py --server.port 8502
```

### 3. Deploy to VM
```bash
./scripts/deploy_to_vm.sh
```

## 📊 Dashboards

- **Dental Dashboard**: Provider segmentation, market analysis
- **Mental Health Dashboard**: (Coming soon)

## 🔐 Security

- RSA key-pair authentication for Snowflake
- 1Password integration for secrets management
- No passwords stored in code or environment variables

## 📁 Project Structure

```
data-pipeline/
├── etl/                 ← ETL pipelines (merged from mental-health-etl)
│   ├── pipelines/       ← Pipeline definitions
│   ├── transforms/      ← Data transformations
│   ├── loaders/         ← Data loaders (GCS, BigQuery, Snowflake)
│   ├── npi_ingestion.py ← NPI data ingestion
│   ├── enrichment_pipeline.py
│   └── validation_pipeline.py
├── infra/               ← Infrastructure as Code (from mental-health-etl)
│   ├── terraform/       ← GCP resources
│   ├── vm-setup/        ← VM configuration
│   └── docker/          ← Container configs
├── marketing/           ← Marketing automation (from mental-health-etl)
│   ├── campaigns/       ← Campaign definitions
│   ├── analytics/       ← Marketing analytics
│   └── integrations/   ← CRM/email integrations
├── warehouse/           ← Data warehouse schemas
│   ├── models/          ← dbt models (from mental-health-etl)
│   ├── seeds/           ← Seed data
│   └── sql/             ← SQL schemas and queries
├── dashboards/          ← Streamlit dashboards
├── looker/              ← Looker models and views
├── utils/               ← Python utilities
└── scripts/             ← Automation scripts
```

## 🛠️ Infrastructure

- **Data Warehouse**: Snowflake (primary), BigQuery (secondary)
- **Storage**: GCS (`gs://dl-ingestion-lake/`)
- **Dashboards**: Streamlit on GCP VM
- **Secrets**: 1Password Business

## 📈 Data Sources

- **NPI Registry**: 9.2M US healthcare providers
- **Wiza**: Contact enrichment (email, LinkedIn)
- **HUD ZIP-County Crosswalk**: Geographic mapping

