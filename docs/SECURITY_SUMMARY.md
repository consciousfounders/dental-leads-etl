# Security Architecture Summary

## Quick Reference

### Current Security Posture: **7/10** ⚠️

**Strengths:**
- ✅ RSA key-pair authentication (Snowflake)
- ✅ Service account keys (BigQuery)
- ✅ GCP Secret Manager integration
- ✅ SOC 2 audit logging (ETL pipelines)
- ✅ Least privilege access (read-only for viz tools)

**Weaknesses:**
- ⚠️ Secrets scattered across multiple systems
- ⚠️ Manual credential rotation (4+ places to update)
- ⚠️ No unified audit trail
- ⚠️ Visualization tools hardcoded to Snowflake

---

## Secret Storage Locations

| Component | Location | Rotation Difficulty |
|-----------|----------|-------------------|
| **ETL Pipelines** | GCP Secret Manager | Easy (single update) |
| **Streamlit** | `.streamlit/secrets.toml` | Manual file edit |
| **Looker** | `config/looker_snowflake.json` + `config/looker_rsa_key.p8` | Manual file edit |
| **Hex** | Hex UI (manual entry) | Manual UI update |
| **VM** | Unknown (needs documentation) | Unknown |

---

## Credential Rotation Procedure

### Snowflake RSA Key Rotation

**Current Process (Manual):**
1. Generate new RSA key pair
2. Update Snowflake user: `ALTER USER HEX_USER SET RSA_PUBLIC_KEY = '...'`
3. Update GCP Secret Manager: `snowflake-private-key`
4. Update `.streamlit/secrets.toml`
5. Update `config/looker_rsa_key.p8`
6. Update Hex connection (manual)
7. Restart services

**Recommended:** Automate via script (see `scripts/rotate_snowflake_key.sh` - TODO)

### BigQuery Service Account Rotation

**Current Process:**
1. Create new service account key
2. Update GCP Secret Manager (if used)
3. Update Hex connection (manual)
4. Revoke old key after verification

**Recommended:** Use short-lived credentials (OAuth) where possible

---

## Access Control Matrix

| User/Service | Snowflake | BigQuery | GCS | Purpose |
|--------------|-----------|----------|-----|---------|
| **ETL Pipeline** | Read/Write | Read/Write | Read/Write | Data ingestion |
| **Streamlit** | Read-only | ❌ None | ❌ None | Visualization |
| **Looker** | Read-only | ❌ None | ❌ None | BI Dashboard |
| **Hex** | Read-only | Read-only | ❌ None | Data Analysis |
| **Hex User (HEX_USER)** | Read-only (HEX_ROLE) | N/A | N/A | Dedicated Hex access |

---

## Security Best Practices Checklist

### ✅ Implemented
- [x] RSA key-pair authentication (Snowflake)
- [x] Service account keys (BigQuery)
- [x] Secrets never in code
- [x] GCP Secret Manager for production
- [x] Environment variable fallback for local dev
- [x] Read-only access for visualization tools
- [x] Dedicated users/roles per use case
- [x] Audit logging (ETL pipelines)

### ⚠️ Needs Improvement
- [ ] Centralized secret storage (all tools)
- [ ] Automated credential rotation
- [ ] Unified audit trail
- [ ] VM security documentation
- [ ] Firewall rules documentation
- [ ] IAM roles documentation
- [ ] Incident response plan

### ❌ Not Implemented
- [ ] Secret rotation automation
- [ ] Multi-factor authentication (where applicable)
- [ ] Network segmentation (VM isolation)
- [ ] Automated security scanning
- [ ] Penetration testing

---

## Recommendations Priority

### P0 (Critical)
1. **Document VM security** - Firewall rules, IAM roles, access controls
2. **Create credential rotation script** - Automate Snowflake key rotation
3. **Centralize secrets** - Migrate all secrets to GCP Secret Manager

### P1 (High)
4. **Unified audit logging** - Single audit trail for all secret accesses
5. **Visualization adapter** - Abstract warehouse choice (see `utils/viz_adapter.py`)
6. **Security runbook** - Document incident response procedures

### P2 (Medium)
7. **Automated security scanning** - Regular dependency/credential scans
8. **Network segmentation** - Isolate VM from public internet
9. **Multi-factor authentication** - Where applicable

---

## Compliance Notes

### SOC 2 Requirements
- ✅ **Access Controls**: Implemented (least privilege)
- ✅ **Audit Logging**: Partial (ETL pipelines only)
- ⚠️ **Secret Management**: Fragmented (needs consolidation)
- ⚠️ **Change Management**: Manual (needs automation)

### HIPAA Considerations (if applicable)
- ⚠️ **Data Encryption**: Verify at-rest and in-transit encryption
- ⚠️ **Access Logging**: Ensure all PHI access is logged
- ⚠️ **Business Associate Agreements**: Verify with all vendors (Snowflake, BigQuery, Hex, Looker)

---

## Quick Links

- [Full Architecture Assessment](./ARCHITECTURE_ASSESSMENT.md)
- [Hex Setup Guide](./HEX_SETUP.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
- [Main Architecture Doc](../ARCHITECTURE.md)

---

*Last Updated: December 2024*


