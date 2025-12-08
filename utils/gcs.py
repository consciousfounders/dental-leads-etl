from google.cloud import storage
import os
from typing import List

class GCSClient:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.bucket_name = os.getenv("GCS_BUCKET_NAME", "dl-ingestion-lake")
        
        # Initialize client (uses GOOGLE_APPLICATION_CREDENTIALS env var)
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)
    
    def list(self, prefix: str = "") -> List[str]:
        """List all files in bucket with optional prefix"""
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]
    
    def download(self, blob_name: str, destination_path: str) -> str:
        """Download a file from GCS to local path"""
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(destination_path)
        print(f"✅ Downloaded {blob_name} to {destination_path}")
        return destination_path
    
    def upload(self, source_path: str, blob_name: str) -> str:
        """Upload a local file to GCS"""
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(source_path)
        print(f"✅ Uploaded {source_path} to gs://{self.bucket_name}/{blob_name}")
        return f"gs://{self.bucket_name}/{blob_name}"
    
    def exists(self, blob_name: str) -> bool:
        """Check if a file exists in GCS"""
        blob = self.bucket.blob(blob_name)
        return blob.exists()
    
    def get_uri(self, blob_name: str) -> str:
        """Get full GCS URI for a blob"""
        return f"gs://{self.bucket_name}/{blob_name}"
