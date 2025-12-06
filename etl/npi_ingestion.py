from utils.gcs import GCSClient
from utils.snowflake import SnowflakeClient

def run():
    print("Running NPI ingestion test...")

    # Initialize clients
    gcs = GCSClient()

    # Example: list GCS files
    files = gcs.list("npi/")
    print("Files in GCS 'npi/' prefix:", files)

if __name__ == "__main__":
    run()
