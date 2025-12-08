import os
from datetime import datetime
from utils.db_adapter import get_db
from utils.gcs import GCSClient

def run():
    """
    Ingest NPI data from GCS into Snowflake RAW layer
    
    Expected structure:
    - GCS path: gs://dl-ingestion-lake/npi/*.csv
    - Target table: RAW.NPI_PROVIDERS
    """
    print(f"🚀 Starting NPI ingestion pipeline at {datetime.now()}")
    
    try:
        # Initialize clients
        gcs = GCSClient()
        
        # List available NPI files
        npi_files = gcs.list("npi/")
        
        if not npi_files:
            print("⚠️  No NPI files found in GCS bucket")
            return
        
        print(f"📂 Found {len(npi_files)} file(s) in gs://{gcs.bucket_name}/npi/")
        for file in npi_files:
            print(f"   - {file}")
        
        # Connect to warehouse
        with get_db() as db:
            # Load SQL template
            sql_path = "sql/raw/load_npi.sql"
            
            if not os.path.exists(sql_path):
                raise FileNotFoundError(f"SQL file not found: {sql_path}")
            
            with open(sql_path, "r") as f:
                sql = f.read()
            
            # Execute load
            print(f"📥 Executing SQL from {sql_path}")
            db.execute(sql)
            
            # Verify row count
            count_sql = "SELECT COUNT(*) FROM RAW.NPI_PROVIDERS"
            result = db.execute(count_sql)
            row_count = result[0][0] if result else 0
            
            print(f"✅ NPI ingestion complete! Total rows: {row_count:,}")
    
    except Exception as e:
        print(f"❌ NPI ingestion failed: {e}")
        raise

if __name__ == "__main__":
    run()
