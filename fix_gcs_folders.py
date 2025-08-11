#!/usr/bin/env python3
"""
Fix GCS Folder Structure
Creates proper folder structure in GCS with actual files
"""

import os
from google.cloud import storage
from gcs_config import get_gcs_config

def fix_gcs_folders():
    """Create proper folder structure in GCS."""
    print("🔧 Fixing GCS folder structure...")
    
    try:
        # Get configuration
        config = get_gcs_config()
        
        # Set credentials
        if config.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
        
        # Create storage client
        client = storage.Client(project=config.project_id)
        bucket = client.bucket(config.bucket_name)
        
        # Required folders with sample files
        folders = {
            'bronze': 'README.md',
            'silver': 'README.md', 
            'gold': 'README.md',
            'logs': 'README.md',
            'temp': 'README.md'
        }
        
        print("   Creating proper folder structure:")
        for folder, filename in folders.items():
            file_path = f"{folder}/{filename}"
            content = f"# {folder.title()} Zone\n\nThis folder contains {folder} data for the data engineering assignment.\n\nCreated: {__import__('datetime').datetime.now().isoformat()}"
            
            blob = bucket.blob(file_path)
            blob.upload_from_string(content, content_type="text/markdown")
            print(f"   ✅ Created: {file_path}")
        
        print("   🎉 GCS folder structure fixed!")
        
        # Verify by listing contents
        print("\n   📋 Verifying folder contents:")
        for folder in folders.keys():
            blobs = list(client.list_blobs(bucket, prefix=f"{folder}/"))
            if blobs:
                print(f"   ✅ {folder}/ - Contains {len(blobs)} file(s)")
                for blob in blobs:
                    print(f"      - {blob.name}")
            else:
                print(f"   ❌ {folder}/ - Empty")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to fix GCS folders: {e}")
        return False

if __name__ == "__main__":
    success = fix_gcs_folders()
    if success:
        print("\n✅ GCS folder structure is properly set up!")
    else:
        print("\n❌ GCS folder setup failed!")
