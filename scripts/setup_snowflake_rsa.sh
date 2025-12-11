#!/bin/bash
# Setup RSA Key-Pair Authentication for Snowflake
# This eliminates the need to store passwords on VMs
#
# Flow:
# 1. Generate RSA key pair locally
# 2. Store private key in 1Password
# 3. Register public key in Snowflake
# 4. Test connection with key-based auth

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
KEYS_DIR="$PROJECT_ROOT/.keys"

# 1Password settings
OP_VAULT="CF – OnPharma Admin"
OP_ITEM="Snowflake - Healthcare Leads"

echo "🔐 Setting up RSA Key-Pair Authentication for Snowflake"
echo ""

# Check dependencies
command -v op &> /dev/null || { echo "❌ 1Password CLI required"; exit 1; }
command -v openssl &> /dev/null || { echo "❌ OpenSSL required"; exit 1; }

# Create keys directory
mkdir -p "$KEYS_DIR"
chmod 700 "$KEYS_DIR"

# Step 1: Generate RSA Key Pair
echo "🔑 Step 1: Generating RSA key pair..."
TIMESTAMP=$(date +%Y%m%d)
PRIVATE_KEY="$KEYS_DIR/snowflake_rsa_key_$TIMESTAMP.p8"
PUBLIC_KEY="$KEYS_DIR/snowflake_rsa_key_$TIMESTAMP.pub"

# Generate 2048-bit RSA private key (PKCS#8 format, no passphrase for automation)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out "$PRIVATE_KEY" -nocrypt
chmod 600 "$PRIVATE_KEY"

# Extract public key
openssl rsa -in "$PRIVATE_KEY" -pubout -out "$PUBLIC_KEY"

echo "   ✅ Private key: $PRIVATE_KEY"
echo "   ✅ Public key: $PUBLIC_KEY"
echo ""

# Step 2: Get the public key content (for Snowflake)
PUBLIC_KEY_CONTENT=$(grep -v "BEGIN PUBLIC" "$PUBLIC_KEY" | grep -v "END PUBLIC" | tr -d '\n')

echo "📋 Step 2: Public key for Snowflake (copy this):"
echo ""
echo "-----BEGIN PUBLIC KEY BLOCK-----"
echo "$PUBLIC_KEY_CONTENT"
echo "-----END PUBLIC KEY BLOCK-----"
echo ""

# Step 3: Store private key in 1Password
echo "🔒 Step 3: Storing private key in 1Password..."
PRIVATE_KEY_CONTENT=$(cat "$PRIVATE_KEY")

# Update the existing item with the private key
op item edit "$OP_ITEM" --vault "$OP_VAULT" \
  "rsa_private_key=$PRIVATE_KEY_CONTENT" \
  "rsa_public_key=$PUBLIC_KEY_CONTENT" \
  "rsa_key_date=$TIMESTAMP" \
  > /dev/null 2>&1

echo "   ✅ Keys stored in 1Password: $OP_ITEM"
echo ""

# Step 4: Generate Snowflake SQL
echo "📝 Step 4: Run this SQL in Snowflake to register the public key:"
echo ""
echo "----------------------------------------"
cat << EOF
-- Run this in Snowflake as ACCOUNTADMIN
ALTER USER CONSCIOUSFOUNDERS SET RSA_PUBLIC_KEY='$PUBLIC_KEY_CONTENT';

-- Verify it was set
DESC USER CONSCIOUSFOUNDERS;
EOF
echo "----------------------------------------"
echo ""

# Step 5: Generate test script
TEST_SCRIPT="$SCRIPT_DIR/test_rsa_auth.py"
cat > "$TEST_SCRIPT" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Test Snowflake RSA key-pair authentication"""

import subprocess
import json
import snowflake.connector

def get_from_1password(item, vault, field):
    """Fetch field from 1Password"""
    result = subprocess.run(
        ["op", "item", "get", item, "--vault", vault, "--fields", field, "--reveal"],
        capture_output=True, text=True
    )
    return result.stdout.strip()

def main():
    vault = "CF – OnPharma Admin"
    item = "Snowflake - Healthcare Leads"
    
    print("🔐 Fetching credentials from 1Password...")
    account = get_from_1password(item, vault, "account")
    user = get_from_1password(item, vault, "username")
    warehouse = get_from_1password(item, vault, "warehouse")
    database = get_from_1password(item, vault, "database")
    schema = get_from_1password(item, vault, "schema")
    private_key = get_from_1password(item, vault, "rsa_private_key")
    
    print(f"   Account: {account}")
    print(f"   User: {user}")
    print("")
    
    print("🔗 Connecting with RSA key...")
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            private_key=private_key.encode(),
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"   ✅ Connected!")
        print(f"   User: {result[0]}")
        print(f"   Role: {result[1]}")
        print(f"   Warehouse: {result[2]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
PYTHON_EOF

chmod +x "$TEST_SCRIPT"

echo "🧪 Step 5: After running the SQL, test with:"
echo "   python3 $TEST_SCRIPT"
echo ""

# Cleanup local keys (they're now in 1Password)
echo "🧹 Step 6: Cleaning up local key files..."
rm -f "$PRIVATE_KEY" "$PUBLIC_KEY"
rmdir "$KEYS_DIR" 2>/dev/null || true
echo "   ✅ Local keys removed (stored in 1Password only)"
echo ""

echo "✅ RSA setup complete!"
echo ""
echo "📋 Summary:"
echo "   1. RSA key pair generated"
echo "   2. Private key stored in 1Password"
echo "   3. Run the SQL above in Snowflake"
echo "   4. Test with: python3 scripts/test_rsa_auth.py"

