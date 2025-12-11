# Troubleshooting Guide

## Snowflake Connection Issues

### Error: 404 Not Found
**Problem:** `290404 (08001): 404 Not Found: post JW33852.snowflakecomputing.com`

**Cause:** Wrong account identifier format. Old-style account IDs don't work with new Snowflake URLs.

**Solution:** Use the org-account format from your Snowflake URL:
- URL: `https://app.snowflake.com/lqrrxbi/pd02365/`
- Account identifier: `lqrrxbi-pd02365`

### Error: Incorrect username or password
**Problem:** `250001 (08001): Failed to connect to DB... Incorrect username or password`

**Cause:** Username doesn't match the Snowflake user.

**Solution:** Check actual username in Snowflake → Admin → Users & Roles. Our username is `consciousfounders`.

### Error: No secrets found
**Problem:** `Error: No secrets found. Valid paths for a secrets.toml file...`

**Cause:** Missing `.streamlit/secrets.toml` file.

**Solution:** Create `.streamlit/secrets.toml` with Snowflake credentials (see below).

---

## Streamlit Configuration

### Local secrets file location
```
.streamlit/secrets.toml
```

### Required secrets format
```toml
[snowflake]
account = "lqrrxbi-pd02365"
user = "consciousfounders"
password = "YOUR_PASSWORD"
warehouse = "COMPUTE_WH"
database = "DENTAL_LEADS"
schema = "CLEAN"

[auth]
password = "buffered"
```

### Restart Streamlit after changing secrets
```bash
pkill -9 -f streamlit
streamlit run dashboards/client_dashboard.py --server.port 8502
```

---

## 1Password Integration

To fetch credentials from 1Password CLI:
```bash
./scripts/fetch_snowflake_creds.sh
```

This will:
1. Authenticate with 1Password
2. Fetch Snowflake credentials
3. Generate `.streamlit/secrets.toml`

---

## Common Issues

| Issue | Solution |
|-------|----------|
| Port 8502 already in use | `pkill -9 -f streamlit` |
| Secrets not loading | Restart Streamlit server |
| Wrong account format | Use `org-account` from URL |
| Permission errors | Run with `--server.headless true` |

