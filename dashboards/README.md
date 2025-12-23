# Dashboard Suite

## Available Dashboards

### 1. Dental Overview (`dental_overview.py`)
Main dashboard for dental leads intelligence and analytics.

**Run:**
```bash
streamlit run dashboards/dental_overview.py
```

### 2. Client Dashboard (`client_dashboard.py`)
Market intelligence dashboard with password protection.

**Run:**
```bash
streamlit run dashboards/client_dashboard.py
```

## Configuration

### Required Environment Variables

```bash
# Database
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password  # or use RSA key
SNOWFLAKE_DATABASE=DENTAL_LEADS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### Streamlit Secrets (Alternative)

Create `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "your_account"
user = "your_user"
password = "your_password"
warehouse = "COMPUTE_WH"
database = "DENTAL_LEADS"
schema = "CLEAN"

[auth]
password = "your_dashboard_password"
```

## Styling

All dashboards use a dark theme. CSS is defined in each dashboard file. For consistency, use:

```python
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117 !important;
    }
</style>
""", unsafe_allow_html=True)
```

## Deployment

See `scripts/deploy_dashboard.sh` for deployment instructions.
