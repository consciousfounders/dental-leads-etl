from google.cloud import storage

class GCSClient:
    def __init__(self, bucket="dl-ingestion-lake"):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)

    def list(self, prefix=""):
        """List files in a given prefix within the GCS bucket."""
        return [blob.name for blob in self.bucket.list_blobs(prefix=prefix)]
