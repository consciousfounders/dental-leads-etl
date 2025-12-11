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
    item = "Snowflake - Dental Leads"
    
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
