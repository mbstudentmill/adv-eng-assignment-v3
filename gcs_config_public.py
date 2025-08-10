#!/usr/bin/env python3
"""
Public Google Cloud Storage Configuration - Advanced Data Engineering Assignment
Settings for GCS bucket and project configuration loaded from environment variables.
"""

import os
from pathlib import Path

class GCSConfig:
    """Configuration for Google Cloud Storage integration."""
    
    def __init__(self):
        # Load from environment variables (no hardcoded values)
        self.project_id = os.environ.get('GCP_PROJECT_ID', 'your-project-id-here')
        self.bucket_name = os.environ.get('GCS_BUCKET_NAME', 'your-bucket-name-here')
        self.region = os.environ.get('GCP_REGION', 'us-central1')
        
        # Data zone paths in GCS
        self.bronze_path = "bronze"
        self.silver_path = "silver" 
        self.gold_path = "gold"
        self.logs_path = "logs"
        self.temp_path = "temp"
        
        # Service account credentials
        self.credentials_path = self._get_credentials_path()
        
    def _get_credentials_path(self) -> str:
        """Get the path to service account credentials."""
        # Look for credentials file in project directory
        project_dir = Path(__file__).parent
        possible_names = [
            "gcp-credentials.json",
            "service-account-key.json", 
            "credentials.json"
        ]
        
        for name in possible_names:
            cred_path = project_dir / name
            if cred_path.exists():
                return str(cred_path)
        
        # If no file found, return None (will use default credentials)
        return None
    
    def get_bucket_uri(self) -> str:
        """Get the full GCS bucket URI."""
        return f"gs://{self.bucket_name}"
    
    def get_data_zone_paths(self) -> dict:
        """Get all data zone paths."""
        return {
            "bronze": f"{self.bucket_name}/{self.bronze_path}",
            "silver": f"{self.bucket_name}/{self.silver_path}",
            "gold": f"{self.bucket_name}/{self.gold_path}",
            "logs": f"{self.bucket_name}/{self.logs_path}",
            "temp": f"{self.bucket_name}/{self.temp_path}"
        }
    
    def validate_config(self) -> bool:
        """Validate that required configuration is present."""
        if not self.project_id or self.project_id == "your-project-id-here":
            print("❌ Project ID not configured. Please set GCP_PROJECT_ID environment variable")
            return False
        
        if not self.bucket_name or self.bucket_name == "your-bucket-name-here":
            print("❌ Bucket name not configured. Please set GCS_BUCKET_NAME environment variable")
            return False
        
        if not self.credentials_path:
            print("⚠️  No service account credentials found. Will use default credentials.")
            print("   Place your JSON credentials file in the project directory or set GOOGLE_APPLICATION_CREDENTIALS")
        
        return True
    
    def is_cloud_ready(self) -> bool:
        """Check if configuration is ready for cloud operations."""
        return self.validate_config() and self.project_id != "your-project-id-here"
    
    def get_local_fallback_paths(self) -> dict:
        """Get local file paths as fallback when cloud is not configured."""
        return {
            "bronze": "data/bronze",
            "silver": "data/silver",
            "gold": "data/gold",
            "logs": "logs",
            "temp": "temp"
        }

def get_gcs_config() -> GCSConfig:
    """Get GCS configuration instance."""
    return GCSConfig()

if __name__ == "__main__":
    config = get_gcs_config()
    print("🔧 GCS Configuration:")
    print(f"   Project ID: {config.project_id}")
    print(f"   Bucket: {config.bucket_name}")
    print(f"   Region: {config.region}")
    print(f"   Credentials: {config.credentials_path}")
    
    if config.validate_config():
        print("✅ Configuration is valid!")
        if config.is_cloud_ready():
            print("☁️  Ready for cloud operations!")
        else:
            print("💻 Will use local file system as fallback")
    else:
        print("❌ Configuration needs attention!")
        print("💡 Set environment variables or use local file system")
