# Healthcare Leads Data Platform

A comprehensive data warehouse and marketing intelligence platform for healthcare provider leads.

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
healthcare-leads-data/
├── dashboards/          ← Streamlit dashboards
├── sql/
│   ├── raw/             ← Data ingestion
│   ├── dental/          ← Dental transformations
│   ├── mental_health/   ← Mental health transformations
│   └── enriched/        ← Enrichment views
├── scripts/             ← Automation scripts
├── utils/               ← Python utilities
└── docker/              ← Container configs
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

