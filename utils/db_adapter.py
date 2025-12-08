import os
from utils.snowflake_client import SnowflakeClient
from utils.bigquery_client import BigQueryClient

def get_db():
    engine = os.getenv("WAREHOUSE_ENGINE", "snowflake").lower()

    if engine == "snowflake":
        return SnowflakeClient()
    elif engine == "bigquery":
        return BigQueryClient()
    else:
        raise ValueError(f"Unknown warehouse engine: {engine}")
