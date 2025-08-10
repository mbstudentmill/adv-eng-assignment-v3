#!/usr/bin/env python3
"""
List Google Cloud Storage Buckets
Shows all available buckets in the project
"""

import os
from google.cloud import storage
from gcs_config import get_gcs_config

def list_gcs_buckets():
    """List all GCS buckets in the project."""
    print("🔍 Listing all GCS buckets in your project...")
    
    try:
        # Get configuration
        config = get_gcs_config()
        print(f"   Project ID: {config.project_id}")
        
        # Set credentials
        if config.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
            print(f"   Using credentials from: {config.credentials_path}")
        
        # Create storage client
        client = storage.Client(project=config.project_id)
        print("   ✅ Storage client created successfully!")
        
        # List all buckets
        print("   📦 Available buckets:")
        buckets = list(client.list_buckets())
        
        if buckets:
            for i, bucket in enumerate(buckets, 1):
                print(f"   {i}. {bucket.name}")
                print(f"      Location: {bucket.location}")
                print(f"      Created: {bucket.time_created}")
                print(f"      Storage Class: {bucket.storage_class}")
                print()
        else:
            print("   ❌ No buckets found in this project!")
            print("   You may need to create a bucket first")
        
        return buckets
        
    except Exception as e:
        print(f"   ❌ Failed to list buckets: {e}")
        return []

if __name__ == "__main__":
    buckets = list_gcs_buckets()
    if buckets:
        print(f"✅ Found {len(buckets)} bucket(s) in your project")
    else:
        print("❌ No buckets found or error occurred")
