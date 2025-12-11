#!/bin/bash
# Fetch Snowflake credentials from 1Password and generate secrets.toml
# Usage: ./scripts/fetch_snowflake_creds.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SECRETS_FILE="$PROJECT_ROOT/.streamlit/secrets.toml"

# 1Password item name for Snowflake credentials
OP_ITEM="Snowflake - Dental Leads"

echo "🔐 Fetching Snowflake credentials from 1Password..."

# Check if 1Password CLI is installed
if ! command -v op &> /dev/null; then
    echo "❌ 1Password CLI (op) not installed."
    echo "   Install with: brew install 1password-cli"
    exit 1
fi

# Check if signed in
if ! op account list &> /dev/null; then
    echo "📱 Please sign in to 1Password..."
    eval $(op signin)
fi

# Try to fetch credentials
# If the item doesn't exist, create it
if ! op item get "$OP_ITEM" &> /dev/null; then
    echo "📝 Creating new 1Password item: $OP_ITEM"
    
    # Prompt for password
    echo -n "Enter Snowflake password: "
    read -s SF_PASSWORD
    echo ""
    
    op item create \
        --category login \
        --title "$OP_ITEM" \
        --vault "Private" \
        "username=consciousfounders" \
        "password=$SF_PASSWORD" \
        "account=lqrrxbi-pd02365" \
        "warehouse=COMPUTE_WH" \
        "database=DENTAL_LEADS" \
        "schema=CLEAN" \
        "website=https://app.snowflake.com/lqrrxbi/pd02365/"
    
    echo "✅ Created 1Password item: $OP_ITEM"
fi

# Fetch credentials from 1Password
echo "📥 Fetching credentials..."
SF_ACCOUNT=$(op item get "$OP_ITEM" --fields account 2>/dev/null || echo "lqrrxbi-pd02365")
SF_USER=$(op item get "$OP_ITEM" --fields username 2>/dev/null || echo "consciousfounders")
SF_PASSWORD=$(op item get "$OP_ITEM" --fields password)
SF_WAREHOUSE=$(op item get "$OP_ITEM" --fields warehouse 2>/dev/null || echo "COMPUTE_WH")
SF_DATABASE=$(op item get "$OP_ITEM" --fields database 2>/dev/null || echo "DENTAL_LEADS")
SF_SCHEMA=$(op item get "$OP_ITEM" --fields schema 2>/dev/null || echo "CLEAN")

# Create .streamlit directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/.streamlit"

# Generate secrets.toml
cat > "$SECRETS_FILE" << EOF
[snowflake]
account = "$SF_ACCOUNT"
user = "$SF_USER"
password = "$SF_PASSWORD"
warehouse = "$SF_WAREHOUSE"
database = "$SF_DATABASE"
schema = "$SF_SCHEMA"

[auth]
password = "buffered"
EOF

echo "✅ Generated $SECRETS_FILE"
echo ""
echo "🚀 Ready! Start the dashboard with:"
echo "   streamlit run dashboards/client_dashboard.py --server.port 8502"

