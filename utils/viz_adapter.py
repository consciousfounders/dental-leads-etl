"""
Visualization Adapter - Unified Interface for Visualization Tools

This adapter abstracts the data warehouse choice, allowing visualization tools
(Streamlit, Looker, Hex) to work with either Snowflake or BigQuery without
code changes.

Usage:
    from utils.viz_adapter import VizAdapter
    
    viz = VizAdapter()
    df = viz.query("SELECT * FROM CLEAN.DENTAL_PROVIDERS LIMIT 100")
    
    # For tools that need native connection
    conn = viz.get_native_connection()
"""

import logging
from typing import Optional, Union
import pandas as pd

from utils.db_adapter import get_db

logger = logging.getLogger(__name__)


class VizAdapter:
    """
    Unified adapter for visualization tools to query any warehouse.
    
    This class provides a consistent interface regardless of whether
    the underlying warehouse is Snowflake or BigQuery.
    
    Features:
    - Unified query interface
    - Automatic warehouse detection
    - Native connection access (for tools that need it)
    - Caching support (via tool-level caching)
    """
    
    def __init__(self, warehouse: Optional[str] = None):
        """
        Initialize the visualization adapter.
        
        Args:
            warehouse: Optional warehouse override ('snowflake' | 'bigquery').
                      If None, uses WAREHOUSE_ENGINE env var.
        """
        import os
        if warehouse:
            os.environ['WAREHOUSE_ENGINE'] = warehouse
        
        self.db = get_db()
        self._warehouse_type = os.getenv('WAREHOUSE_ENGINE', 'snowflake').lower()
        logger.info(f"✅ VizAdapter initialized for {self._warehouse_type}")
    
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return pandas DataFrame.
        
        This is the primary method for visualization tools.
        Works identically for Snowflake and BigQuery.
        
        Args:
            sql: SQL query string
            
        Returns:
            pandas DataFrame with query results
            
        Example:
            df = viz.query("SELECT * FROM CLEAN.DENTAL_PROVIDERS LIMIT 100")
        """
        try:
            logger.info(f"📊 Executing query on {self._warehouse_type}...")
            df = self.db.fetch_df(sql)
            logger.info(f"✅ Query returned {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            logger.error(f"   SQL: {sql[:200]}...")
            raise
    
    def execute(self, sql: str) -> Optional[list]:
        """
        Execute SQL query and return raw results (list of tuples).
        
        Useful for non-SELECT queries or when you need raw tuples.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of tuples, or None for DDL/DML queries
        """
        try:
            logger.info(f"⚙️  Executing SQL on {self._warehouse_type}...")
            results = self.db.execute(sql)
            if results:
                logger.info(f"✅ Query returned {len(results)} rows")
            else:
                logger.info("✅ Query executed successfully (no results)")
            return results
        except Exception as e:
            logger.error(f"❌ Execution failed: {e}")
            raise
    
    def get_native_connection(self):
        """
        Get the native connection object for tools that need it.
        
        Returns:
            - Snowflake connector object (if using Snowflake)
            - BigQuery client object (if using BigQuery)
            
        Note: Most visualization tools should use query() instead.
        This is only needed for tools that require native connection objects.
        """
        if self._warehouse_type == 'snowflake':
            # Return Snowflake connection
            if hasattr(self.db, 'conn'):
                return self.db.conn
            else:
                # Force connection if lazy
                _ = self.db.conn  # Trigger lazy connection
                return self.db.conn
        elif self._warehouse_type == 'bigquery':
            # Return BigQuery client
            return self.db.client
        else:
            raise ValueError(f"Unknown warehouse type: {self._warehouse_type}")
    
    @property
    def warehouse_type(self) -> str:
        """Get the current warehouse type ('snowflake' | 'bigquery')"""
        return self._warehouse_type
    
    def close(self):
        """Close the database connection"""
        if hasattr(self.db, 'close'):
            self.db.close()
            logger.info("✅ Connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-close connection"""
        self.close()


# Convenience function for quick queries
def quick_query(sql: str, warehouse: Optional[str] = None) -> pd.DataFrame:
    """
    Quick query function - creates adapter, queries, and closes.
    
    Useful for one-off queries in notebooks or scripts.
    
    Args:
        sql: SQL query string
        warehouse: Optional warehouse override
        
    Returns:
        pandas DataFrame
        
    Example:
        df = quick_query("SELECT COUNT(*) FROM CLEAN.DENTAL_PROVIDERS")
    """
    with VizAdapter(warehouse=warehouse) as viz:
        return viz.query(sql)


