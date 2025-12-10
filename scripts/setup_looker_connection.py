#!/usr/bin/env python3
"""
Setup Looker Database Connection via API

This script creates the Snowflake connection in Looker programmatically.
Requires: Looker API credentials (client_id, client_secret)
"""

import os
import sys
import json
import subprocess
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import looker_sdk
from looker_sdk import models40 as models


def get_api_credentials():
    """Get Looker API credentials from 1Password or environment"""
    
    # Try environment variables first
    client_id = os.getenv('LOOKER_CLIENT_ID')
    client_secret = os.getenv('LOOKER_CLIENT_SECRET')
    base_url = os.getenv('LOOKER_BASE_URL')
    
    if client_id and client_secret and base_url:
        return client_id, client_secret, base_url
    
    # Try 1Password
    print("🔑 Checking 1Password for Looker API credentials...")
    try:
        result = subprocess.run(
            ['op', 'item', 'get', 'Looker API', '--format', 'json'],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            item = json.loads(result.stdout)
            fields = {f.get('label', f.get('id')): f.get('value') for f in item.get('fields', [])}
            return (
                fields.get('client_id') or fields.get('credential'),
                fields.get('client_secret') or fields.get('password'),
                fields.get('base_url') or fields.get('website')
            )
    except Exception as e:
        print(f"   Could not fetch from 1Password: {e}")
    
    # Prompt user
    print("\n📝 Enter Looker API credentials:")
    base_url = input("   Looker URL (e.g., https://company.cloud.looker.com): ").strip()
    client_id = input("   Client ID: ").strip()
    client_secret = input("   Client Secret: ").strip()
    
    # Offer to save to 1Password
    save = input("\n   Save to 1Password? (y/n): ").strip().lower()
    if save == 'y':
        save_to_1password(client_id, client_secret, base_url)
    
    return client_id, client_secret, base_url


def save_to_1password(client_id: str, client_secret: str, base_url: str):
    """Save Looker API credentials to 1Password"""
    try:
        subprocess.run(['op', 'item', 'delete', 'Looker API', '--vault', 'Private'], capture_output=True)
    except:
        pass
    
    cmd = [
        'op', 'item', 'create',
        '--category', 'api_credential',
        '--title', 'Looker API',
        '--vault', 'Private',
        f'credential={client_id}',
        f'password={client_secret}',
        f'website={base_url}',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("   ✅ Saved to 1Password")
    else:
        print(f"   ⚠️  Could not save to 1Password: {result.stderr}")


def load_snowflake_config():
    """Load Snowflake connection config from local file"""
    config_path = Path(__file__).parent.parent / 'config' / 'looker_snowflake.json'
    
    if not config_path.exists():
        print(f"❌ Snowflake config not found: {config_path}")
        print("   Run setup_looker_snowflake.py first")
        sys.exit(1)
    
    with open(config_path) as f:
        return json.load(f)


def load_private_key():
    """Load the private key for Snowflake auth"""
    key_path = Path(__file__).parent.parent / 'config' / 'looker_rsa_key.p8'
    
    if not key_path.exists():
        print(f"❌ Private key not found: {key_path}")
        print("   Run setup_looker_snowflake.py first")
        sys.exit(1)
    
    with open(key_path) as f:
        return f.read()


def create_connection(sdk, snowflake_config: dict, private_key: str):
    """Create the Snowflake connection in Looker"""
    
    connection_name = "dental_leads_snowflake"
    
    # Check if connection already exists
    print(f"\n🔍 Checking for existing connection '{connection_name}'...")
    try:
        existing = sdk.connection(connection_name)
        if existing:
            print(f"   Found existing connection. Updating...")
            # Delete and recreate (simpler than updating)
            sdk.delete_connection(connection_name)
            print(f"   ✅ Deleted old connection")
    except:
        print(f"   No existing connection found")
    
    # Create new connection
    print(f"\n📊 Creating Snowflake connection...")
    
    connection = models.WriteDBConnection(
        name=connection_name,
        dialect_name="snowflake",
        host=snowflake_config['host'],
        port="443",
        database=snowflake_config['database'],
        schema=snowflake_config['schema'],
        username=snowflake_config['username'],
        certificate=private_key,  # Private key for key-pair auth
        file_type=".p8",
        jdbc_additional_params=f"warehouse={snowflake_config['warehouse']}&role={snowflake_config['role']}",
        ssl=True,
        verify_ssl=True,
        tmp_db_name="",
        max_connections=25,
        pool_timeout=120,
    )
    
    try:
        result = sdk.create_connection(connection)
        print(f"   ✅ Connection created: {result.name}")
        return result
    except Exception as e:
        print(f"   ❌ Failed to create connection: {e}")
        raise


def test_connection(sdk, connection_name: str):
    """Test the connection"""
    print(f"\n🧪 Testing connection...")
    
    try:
        results = sdk.test_connection(connection_name)
        
        all_passed = True
        for test in results:
            status = "✅" if test.status == "success" else "❌"
            print(f"   {status} {test.name}: {test.message or 'OK'}")
            if test.status != "success":
                all_passed = False
        
        return all_passed
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        return False


def main():
    print("=" * 60)
    print("LOOKER CONNECTION SETUP VIA API")
    print("=" * 60)
    
    # 1. Get API credentials
    client_id, client_secret, base_url = get_api_credentials()
    
    if not all([client_id, client_secret, base_url]):
        print("❌ Missing API credentials")
        sys.exit(1)
    
    # 2. Initialize SDK
    print(f"\n🔗 Connecting to Looker at {base_url}...")
    
    os.environ['LOOKERSDK_BASE_URL'] = base_url
    os.environ['LOOKERSDK_CLIENT_ID'] = client_id
    os.environ['LOOKERSDK_CLIENT_SECRET'] = client_secret
    os.environ['LOOKERSDK_VERIFY_SSL'] = 'true'
    
    try:
        sdk = looker_sdk.init40()
        me = sdk.me()
        print(f"   ✅ Connected as: {me.display_name} ({me.email})")
    except Exception as e:
        print(f"   ❌ Failed to connect: {e}")
        sys.exit(1)
    
    # 3. Load Snowflake config
    print("\n❄️  Loading Snowflake configuration...")
    snowflake_config = load_snowflake_config()
    private_key = load_private_key()
    print(f"   ✅ Config loaded for {snowflake_config['host']}")
    
    # 4. Create connection
    connection = create_connection(sdk, snowflake_config, private_key)
    
    # 5. Test connection
    if test_connection(sdk, connection.name):
        print("\n" + "=" * 60)
        print("✅ SUCCESS! Snowflake connection is ready in Looker")
        print("=" * 60)
        print(f"\nConnection name: {connection.name}")
        print(f"Database: {snowflake_config['database']}")
        print(f"Schema: {snowflake_config['schema']}")
        print("\nNext: Import the LookML model from looker/ directory")
    else:
        print("\n⚠️  Connection created but tests failed. Check settings in Looker UI.")


if __name__ == "__main__":
    main()

