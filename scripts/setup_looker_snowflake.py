#!/usr/bin/env python3
"""
Setup Looker Snowflake Connection with Key-Pair Auth + 1Password

This script:
1. Generates RSA key pair for Looker service account
2. Creates LOOKER_USER in Snowflake with public key auth
3. Saves credentials to 1Password
4. Saves keys locally to config/ (gitignored)
"""

import os
import sys
import subprocess
import json
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend


def generate_key_pair():
    """Generate RSA key pair for Snowflake authentication"""
    print("\n🔐 Generating RSA key pair...")
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    
    # Serialize private key (unencrypted for Looker compatibility)
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')
    
    # Serialize public key
    public_key = private_key.public_key()
    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8')
    
    print("   ✅ Key pair generated")
    return private_key_pem, public_key_pem


def setup_snowflake_user(public_key_pem: str):
    """Create/update Looker user in Snowflake with public key"""
    from utils.snowflake_client import SnowflakeClient
    
    print("\n❄️  Configuring Snowflake...")
    
    # Extract just the key content (remove header/footer)
    key_lines = public_key_pem.strip().split('\n')
    key_content = ''.join(key_lines[1:-1])  # Remove BEGIN/END lines
    
    with SnowflakeClient() as client:
        # Create role
        print("   Creating LOOKER_ROLE...")
        client.execute('CREATE ROLE IF NOT EXISTS LOOKER_ROLE')
        
        # Grant warehouse
        client.execute('GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOOKER_ROLE')
        
        # Grant database
        client.execute('GRANT USAGE ON DATABASE DENTAL_LEADS TO ROLE LOOKER_ROLE')
        
        # Grant schemas
        for schema in ['RAW', 'CLEAN', 'ENRICHED']:
            try:
                client.execute(f'GRANT USAGE ON SCHEMA DENTAL_LEADS.{schema} TO ROLE LOOKER_ROLE')
                client.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA DENTAL_LEADS.{schema} TO ROLE LOOKER_ROLE')
                client.execute(f'GRANT SELECT ON FUTURE TABLES IN SCHEMA DENTAL_LEADS.{schema} TO ROLE LOOKER_ROLE')
            except:
                pass
        
        # Grant views
        try:
            client.execute('GRANT SELECT ON ALL VIEWS IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE')
            client.execute('GRANT SELECT ON FUTURE VIEWS IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE')
        except:
            pass
        
        # Create user with RSA public key
        print("   Creating LOOKER_USER with key-pair auth...")
        try:
            client.execute(f'''
            CREATE USER IF NOT EXISTS LOOKER_USER
                RSA_PUBLIC_KEY = '{key_content}'
                DEFAULT_ROLE = LOOKER_ROLE
                DEFAULT_WAREHOUSE = COMPUTE_WH
                DEFAULT_NAMESPACE = DENTAL_LEADS.CLEAN
            ''')
        except Exception as e:
            if 'already exists' in str(e).lower():
                # Update existing user's public key
                client.execute(f"ALTER USER LOOKER_USER SET RSA_PUBLIC_KEY = '{key_content}'")
            else:
                raise
        
        # Grant role
        client.execute('GRANT ROLE LOOKER_ROLE TO USER LOOKER_USER')
        
        # Get account info
        account_info = client.execute('SELECT CURRENT_ACCOUNT(), CURRENT_REGION()')
        account = account_info[0][0]
        region = account_info[0][1]
        
        print("   ✅ Snowflake configured")
        
        return {
            'account': account,
            'region': region,
            'host': f"{account}.snowflakecomputing.com",
            'database': 'DENTAL_LEADS',
            'schema': 'CLEAN',
            'warehouse': 'COMPUTE_WH',
            'username': 'LOOKER_USER',
            'role': 'LOOKER_ROLE'
        }


def save_to_1password(private_key: str, connection_info: dict):
    """Save credentials to 1Password"""
    print("\n🔑 Saving to 1Password...")
    
    # Check if op is logged in
    result = subprocess.run(['op', 'account', 'list'], capture_output=True, text=True)
    if result.returncode != 0:
        print("   ⚠️  Please sign in to 1Password CLI first:")
        print("      eval $(op signin)")
        return False
    
    # Create the item in 1Password
    item_name = "Looker - Snowflake Connection"
    
    # Build the op command
    try:
        # First, try to delete existing item if it exists
        subprocess.run(
            ['op', 'item', 'delete', item_name, '--vault', 'Private'],
            capture_output=True
        )
    except:
        pass
    
    # Create new item with all fields
    cmd = [
        'op', 'item', 'create',
        '--category', 'database',
        '--title', item_name,
        '--vault', 'Private',
        f'host={connection_info["host"]}',
        f'database={connection_info["database"]}',
        f'username={connection_info["username"]}',
        f'warehouse={connection_info["warehouse"]}',
        f'role={connection_info["role"]}',
        f'schema={connection_info["schema"]}',
        f'account={connection_info["account"]}',
        f'private_key[password]={private_key}',
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ Saved to 1Password vault 'Private'")
        return True
    else:
        print(f"   ⚠️  1Password save failed: {result.stderr}")
        print("   Credentials will be saved locally only")
        return False


def save_locally(private_key: str, public_key: str, connection_info: dict):
    """Save keys and config locally"""
    print("\n💾 Saving locally...")
    
    config_dir = Path(__file__).parent.parent / 'config'
    config_dir.mkdir(exist_ok=True)
    
    # Save private key
    private_key_path = config_dir / 'looker_rsa_key.p8'
    with open(private_key_path, 'w') as f:
        f.write(private_key)
    os.chmod(private_key_path, 0o600)
    print(f"   ✅ Private key: {private_key_path}")
    
    # Save public key
    public_key_path = config_dir / 'looker_rsa_key.pub'
    with open(public_key_path, 'w') as f:
        f.write(public_key)
    print(f"   ✅ Public key: {public_key_path}")
    
    # Save connection info
    connection_path = config_dir / 'looker_snowflake.json'
    with open(connection_path, 'w') as f:
        json.dump({
            **connection_info,
            'private_key_path': str(private_key_path),
            'auth_type': 'key_pair',
            'created_at': datetime.utcnow().isoformat()
        }, f, indent=2)
    print(f"   ✅ Connection info: {connection_path}")
    
    return private_key_path


def print_looker_instructions(connection_info: dict, private_key_path: Path):
    """Print instructions for Looker setup"""
    print("\n" + "=" * 60)
    print("📊 LOOKER CONNECTION SETUP")
    print("=" * 60)
    print(f"""
In Looker Admin > Database > Connections, create new connection:

   Name:           dental_leads_snowflake
   Dialect:        Snowflake
   Host:           {connection_info['host']}
   Port:           443
   Database:       {connection_info['database']}
   Schema:         {connection_info['schema']}
   
   Authentication: Key Pair
   Username:       {connection_info['username']}
   Private Key:    (paste contents of {private_key_path})
   
   Additional Settings:
   - Warehouse:    {connection_info['warehouse']}
   - Role:         {connection_info['role']}

To get the private key for Looker:
   cat {private_key_path}

Or from 1Password:
   op item get "Looker - Snowflake Connection" --field private_key
""")


def main():
    print("=" * 60)
    print("LOOKER SNOWFLAKE SETUP - Key Pair Auth + 1Password")
    print("=" * 60)
    
    # Set env to skip secret manager
    os.environ['SKIP_SECRET_MANAGER'] = 'true'
    
    # 1. Generate keys
    private_key, public_key = generate_key_pair()
    
    # 2. Setup Snowflake
    connection_info = setup_snowflake_user(public_key)
    
    # 3. Save to 1Password
    save_to_1password(private_key, connection_info)
    
    # 4. Save locally
    private_key_path = save_locally(private_key, public_key, connection_info)
    
    # 5. Print instructions
    print_looker_instructions(connection_info, private_key_path)
    
    print("\n✅ Setup complete!")


if __name__ == "__main__":
    main()

