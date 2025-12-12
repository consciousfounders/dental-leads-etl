# Hex Data Connection Setup Guide

## Overview

This guide covers connecting [Hex](https://hex.tech) to the Healthcare Leads data sources:
- **BigQuery** - Google Cloud data warehouse
- **Snowflake** - Cloud data platform

---

## BigQuery Connection

### Prerequisites
- GCP Project with BigQuery enabled
- Service account with appropriate permissions

### Required IAM Roles
- `roles/bigquery.dataViewer` - Read access to data
- `roles/bigquery.jobUser` - Run queries

### Setup Steps

1. **Create Service Account** (GCP Console or CLI):
   ```bash
   gcloud iam service-accounts create hex-bigquery \
     --description="Hex BigQuery access" \
     --display-name="Hex BigQuery"
   ```

2. **Grant Permissions**:
   ```bash
   PROJECT_ID=$(gcloud config get-value project)
   
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:hex-bigquery@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:hex-bigquery@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"
   ```

3. **Download Key**:
   ```bash
   gcloud iam service-accounts keys create hex-bigquery-key.json \
     --iam-account=hex-bigquery@$PROJECT_ID.iam.gserviceaccount.com
   ```

4. **Configure in Hex**:
   - Navigate to: Settings → Data connections → Add connection → BigQuery
   - Upload the JSON key file
   - Set default dataset: `dental_leads`

### Connection Details
| Field | Value |
|-------|-------|
| Connection Name | `Healthcare Leads BigQuery` |
| Project ID | Your GCP project ID |
| Default Dataset | `dental_leads` |

---

## Snowflake Connection

### Prerequisites
- Snowflake account with active warehouse
- User credentials (password or RSA key-pair)

### Option 1: Create Dedicated Hex User (Recommended)

Run in Snowflake:

```sql
-- 1. Create role for Hex
CREATE ROLE IF NOT EXISTS HEX_ROLE;

-- 2. Create user for Hex
CREATE USER IF NOT EXISTS HEX_USER
  DEFAULT_ROLE = HEX_ROLE
  DEFAULT_WAREHOUSE = DL_WH
  DEFAULT_NAMESPACE = DENTAL_LEADS.RAW
  MUST_CHANGE_PASSWORD = FALSE;

-- 3. Grant warehouse access
GRANT USAGE ON WAREHOUSE DL_WH TO ROLE HEX_ROLE;

-- 4. Grant database/schema access
GRANT USAGE ON DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- 5. Grant table access (read-only)
GRANT SELECT ON ALL TABLES IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT SELECT ON ALL VIEWS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- 6. Assign role to user
GRANT ROLE HEX_ROLE TO USER HEX_USER;

-- 7. Set authentication (choose one):
-- Option A: Password authentication
ALTER USER HEX_USER SET PASSWORD = 'YourSecurePassword!';

-- Option B: RSA Key-Pair (more secure)
-- Generate key pair first, then:
-- ALTER USER HEX_USER SET RSA_PUBLIC_KEY = 'MIIBIjAN...';
```

### Option 2: RSA Key-Pair Setup

Generate a new key pair for Hex:

```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out hex_rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in hex_rsa_key.p8 -pubout -out hex_rsa_key.pub

# Get public key content (for Snowflake)
cat hex_rsa_key.pub | grep -v "PUBLIC KEY" | tr -d '\n'
```

Then in Snowflake:
```sql
ALTER USER HEX_USER SET RSA_PUBLIC_KEY = '<public_key_content>';
```

### Connection Details
| Field | Value |
|-------|-------|
| Connection Name | `Healthcare Leads Snowflake` |
| Account | `<your-account>.<region>` |
| User | `HEX_USER` |
| Warehouse | `DL_WH` |
| Database | `DENTAL_LEADS` |
| Schema | `RAW` |
| Role | `HEX_ROLE` |

---

## Available Schemas & Tables

### BigQuery (`dental_leads` dataset)
- Dental provider data
- See `sql/bigquery/` for schema definitions

### Snowflake (`DENTAL_LEADS` database)

| Schema | Description |
|--------|-------------|
| `RAW` | Raw NPI data, ingested from CMS |
| `ENRICHED` | Validated and enriched provider data |

Key Tables:
- `RAW.NPI_PROVIDERS` - Raw NPI registry data
- `ENRICHED.DENTAL_PROVIDERS` - Clean dental provider records

---

## Security Best Practices

1. **Use dedicated service accounts** - Don't share credentials with other services
2. **Apply least privilege** - Only grant SELECT access unless writes are needed
3. **Rotate credentials** - Rotate keys/passwords periodically
4. **Use RSA key-pair** - More secure than passwords for production
5. **Audit access** - Monitor query logs in both platforms

---

## Troubleshooting

### BigQuery
- **Permission denied**: Ensure service account has `bigquery.dataViewer` and `bigquery.jobUser` roles
- **Dataset not found**: Check project ID and dataset name match

### Snowflake
- **Account not found**: Use full account identifier with region (e.g., `abc123.us-east-1`)
- **Warehouse suspended**: Warehouse may be set to auto-suspend; Hex will auto-resume
- **Key format error**: Ensure private key is in PKCS8 format (`.p8`)

---

## Support

For issues with:
- **Hex platform**: [Hex Documentation](https://learn.hex.tech/docs)
- **BigQuery**: Check GCP Console → BigQuery → Query history
- **Snowflake**: Check Snowflake Console → Activity → Query History


