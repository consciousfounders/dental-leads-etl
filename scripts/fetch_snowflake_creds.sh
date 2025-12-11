#!/bin/bash
# Fetch Snowflake credentials from 1Password and generate secrets.toml
# Supports RSA key-pair auth (preferred) and password auth (fallback)
# Usage: ./scripts/fetch_snowflake_creds.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SECRETS_FILE="$PROJECT_ROOT/.streamlit/secrets.toml"

# 1Password item details
OP_VAULT="CF – OnPharma Admin"
OP_ITEM="Snowflake - Dental Leads"

echo "🔐 Fetching Snowflake credentials from 1Password..."

# Check if 1Password CLI is installed
if ! command -v op &> /dev/null; then
    echo "❌ 1Password CLI (op) not installed."
    echo "   Install with: brew install 1password-cli"
    exit 1
fi

# Check if signed in (will prompt for auth if needed)
if ! op account list &> /dev/null 2>&1; then
    echo "📱 Please sign in to 1Password..."
    eval $(op signin)
fi

# Fetch credentials
echo "📥 Fetching credentials from '$OP_ITEM'..."

SF_ACCOUNT=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields account --reveal)
SF_USER=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields username --reveal)
SF_WAREHOUSE=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields warehouse --reveal)
SF_DATABASE=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields database --reveal)
SF_SCHEMA=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields schema --reveal)

# Try to get RSA private key (preferred)
SF_RSA_KEY=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields rsa_private_key --reveal 2>/dev/null || echo "")

# Also get password as fallback
SF_PASSWORD=$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields password --reveal 2>/dev/null || echo "")

# Validate we got something
if [ -z "$SF_RSA_KEY" ] && [ -z "$SF_PASSWORD" ]; then
    echo "❌ No RSA key or password found in 1Password"
    exit 1
fi

# Create .streamlit directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/.streamlit"

# Generate secrets.toml
if [ -n "$SF_RSA_KEY" ]; then
    echo "   🔑 Using RSA key-pair authentication (more secure)"
    # Clean the key - remove any leading/trailing quotes from 1Password
    SF_RSA_KEY_CLEAN=$(echo "$SF_RSA_KEY" | sed 's/^"//; s/"$//')
    cat > "$SECRETS_FILE" << EOF
[snowflake]
account = "$SF_ACCOUNT"
user = "$SF_USER"
warehouse = "$SF_WAREHOUSE"
database = "$SF_DATABASE"
schema = "$SF_SCHEMA"
rsa_private_key = """
$SF_RSA_KEY_CLEAN
"""

[auth]
password = "buffered"
EOF
else
    echo "   🔒 Using password authentication"
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
fi

echo "✅ Generated $SECRETS_FILE"
echo ""
echo "🚀 Ready! Start the dashboard with:"
echo "   streamlit run dashboards/client_dashboard.py --server.port 8502"
