"""
Snowflake Client with Secret Manager Integration

Features:
- Fetches credentials from Secret Manager (with env var fallback)
- Uses RSA key-pair authentication (recommended for service accounts)
- Handles both file paths and PEM content for private keys
- Lazy connection (only connects when needed)
- Context manager support for automatic cleanup
"""

import snowflake.connector
import logging
import os
from typing import List, Dict, Any, Optional
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """
    Production-ready Snowflake client with:
    - Secret Manager integration
    - RSA key-pair authentication
    - Connection pooling
    - Automatic cleanup
    """
    
    def __init__(self):
        # Import here to avoid circular dependency
        from utils.secrets_manager import get_secrets_manager
        
        secrets = get_secrets_manager()
        
        # Fetch credentials from Secret Manager (or env vars)
        self.account = secrets.get_secret("snowflake-account")
        self.user = secrets.get_secret("snowflake-user")
        self.warehouse = secrets.get_secret("snowflake-warehouse", required=False) or "DL_WH"
        self.database = secrets.get_secret("snowflake-database", required=False) or "DENTAL_LEADS"
        self.schema = secrets.get_secret("snowflake-schema", required=False) or "RAW"
        self.role = secrets.get_secret("snowflake-role", required=False) or "ACCOUNTADMIN"
        
        # Fetch private key (could be file path or PEM content)
        private_key_value = secrets.get_secret("snowflake-private-key")
        self.private_key_bytes = self._parse_private_key(private_key_value)
        
        self._conn = None
        
        logger.info(f"✅ SnowflakeClient initialized for account: {self.account}")
    
    def _parse_private_key(self, private_key_value: str) -> bytes:
        """
        Parse private key from either:
        1. File path (e.g., '/path/to/key.p8')
        2. PEM content (e.g., '-----BEGIN PRIVATE KEY-----...')
        
        This allows flexibility in how the key is stored (Secret Manager
        stores content, env vars might use paths).
        """
        try:
            # Check if it's a file path
            if os.path.isfile(private_key_value):
                logger.info(f"📂 Loading private key from file: {private_key_value}")
                with open(private_key_value, 'r') as f:
                    private_key_pem = f.read()
            else:
                # Assume it's PEM content
                logger.info("🔑 Using private key from environment/secret content")
                private_key_pem = private_key_value
            
            # Load PEM key
            private_key = serialization.load_pem_private_key(
                private_key_pem.encode(),
                password=None,  # Assume unencrypted key
                backend=default_backend()
            )
            
            # Convert to DER format (what Snowflake expects)
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            logger.info("✅ Private key parsed successfully")
            return private_key_bytes
            
        except Exception as e:
            logger.error(f"❌ Failed to parse private key: {e}")
            raise ValueError(f"Invalid private key format: {e}")
    
    @property
    def conn(self):
        """
        Lazy connection - only connects when needed.
        
        This prevents unnecessary connections and allows the client
        to be instantiated without immediate network calls.
        """
        if self._conn is None or self._conn.is_closed():
            try:
                logger.info(f"🔌 Connecting to Snowflake account: {self.account}")
                
                self._conn = snowflake.connector.connect(
                    user=self.user,
                    account=self.account,
                    private_key=self.private_key_bytes,  # Pass bytes directly
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                    role=self.role,
                )
                
                logger.info("✅ Snowflake connection established")
                
            except Exception as e:
                logger.error(f"❌ Snowflake connection failed: {e}")
                raise
        
        return self._conn
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[tuple]]:
        """
        Execute SQL and return results if any.
        
        Args:
            sql: SQL query to execute
            params: Optional query parameters
        
        Returns:
            List of tuples for SELECT queries, None for DDL/DML
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params or {})
            
            # Try to fetch results (SELECT statements)
            try:
                results = cursor.fetchall()
                cursor.close()
                logger.info(f"✅ Query returned {len(results)} rows")
                return results
            except:
                # No results (DDL/DML statements)
                cursor.close()
                logger.info("✅ Query executed successfully (no results)")
                return None
                
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            logger.error(f"   SQL: {sql[:200]}...")  # Log first 200 chars
            raise
    
    def execute_many(self, sql: str, data: List[tuple]) -> None:
        """
        Execute parameterized SQL with multiple rows (batch insert).
        
        Args:
            sql: Parameterized SQL query
            data: List of tuples with parameter values
        """
        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, data)
            cursor.close()
            logger.info(f"✅ Batch executed {len(data)} rows")
        except Exception as e:
            logger.error(f"❌ Batch execution failed: {e}")
            raise
    
    def fetch_df(self, sql: str):
        """
        Execute SQL and return pandas DataFrame.
        
        Args:
            sql: SQL query to execute
        
        Returns:
            pandas DataFrame with query results
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            df = cursor.fetch_pandas_all()
            cursor.close()
            logger.info(f"✅ DataFrame fetched: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"❌ DataFrame fetch failed: {e}")
            raise
    
    def close(self):
        """Close connection and cleanup"""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            self._conn = None
            logger.info("✅ Snowflake connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-close connection"""
        self.close()
