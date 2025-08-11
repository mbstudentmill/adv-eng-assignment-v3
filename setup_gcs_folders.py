#!/usr/bin/env python3
"""
Setup GCS Folder Structure
Creates the required bronze/silver/gold/logs/temp folders in GCS
"""

import os
from google.cloud import storage
from gcs_config import get_gcs_config

def setup_gcs_folders():
    """Create the required folder structure in GCS."""
    print("📁 Setting up GCS folder structure...")
    
    try:
        # Get configuration
        config = get_gcs_config()
        print(f"   Project ID: {config.project_id}")
        print(f"   Bucket: {config.bucket_name}")
        
        # Set credentials
        if config.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
        
        # Create storage client
        client = storage.Client(project=config.project_id)
        bucket = client.bucket(config.bucket_name)
        
        # Required folders
        required_folders = ['bronze', 'silver', 'gold', 'logs', 'temp']
        
        print("   Creating folders:")
        for folder in required_folders:
            # Create a "placeholder" blob to represent the folder
            folder_blob = bucket.blob(f"{folder}/")
            
            if not folder_blob.exists():
                # Create an empty file to represent the folder
                placeholder_blob = bucket.blob(f"{folder}/.placeholder")
                placeholder_blob.upload_from_string("", content_type="text/plain")
                print(f"   ✅ Created: {folder}/")
            else:
                print(f"   ℹ️  Already exists: {folder}/")
        
        print("   🎉 GCS folder structure setup complete!")
        
        # Verify the structure
        print("\n   📋 Verifying folder structure:")
        blobs = list(client.list_blobs(bucket, delimiter='/'))
        
        folders = set()
        for blob in blobs:
            if blob.name.endswith('/') or '/' in blob.name:
                folder_name = blob.name.split('/')[0]
                folders.add(folder_name)
        
        for folder in required_folders:
            if folder in folders:
                print(f"   ✅ {folder}/ - Ready")
            else:
                print(f"   ❌ {folder}/ - Missing")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to setup GCS folders: {e}")
        return False

if __name__ == "__main__":
    success = setup_gcs_folders()
    if success:
        print("\n✅ GCS folder structure is ready!")
    else:
        print("\n❌ GCS folder setup failed!")
