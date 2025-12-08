from google.cloud import bigquery
import os
from typing import List, Optional, Dict, Any

class BigQueryClient:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset = os.getenv("BIGQUERY_DATASET", "dental_leads")
        self.client = bigquery.Client(project=self.project_id)
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[tuple]]:
        """Execute SQL and return results"""
        try:
            query_job = self.client.query(sql)
            results = query_job.result()
            return [tuple(row.values()) for row in results]
        except Exception as e:
            print(f"❌ BigQuery execution error: {e}")
            raise
    
    def fetch_df(self, sql: str):
        """Execute SQL and return pandas DataFrame"""
        return self.client.query(sql).to_dataframe()
    
    def close(self):
        """BigQuery client doesn't need explicit close"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
