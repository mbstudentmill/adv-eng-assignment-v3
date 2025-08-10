#!/usr/bin/env python3
"""
Test Google Cloud Storage Connection
Verifies that we can connect to GCS and access the bucket
"""

import os
from google.cloud import storage
from google.auth import default
from gcs_config import get_gcs_config

def test_gcs_connection():
    """Test connection to Google Cloud Storage."""
    print("🔧 Testing Google Cloud Storage Connection...")
    
    try:
        # Get configuration
        config = get_gcs_config()
        print(f"   Project ID: {config.project_id}")
        print(f"   Bucket: {config.bucket_name}")
        print(f"   Region: {config.region}")
        
        # Check if credentials are available
        if config.credentials_path:
            print(f"   Using credentials from: {config.credentials_path}")
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
        else:
            print("   Using default credentials")
        
        # Create storage client
        print("   Creating storage client...")
        client = storage.Client(project=config.project_id)
        print("   ✅ Storage client created successfully!")
        
        # Test bucket access
        print("   Testing bucket access...")
        bucket = client.bucket(config.bucket_name)
        
        if bucket.exists():
            print(f"   ✅ Bucket '{config.bucket_name}' exists and is accessible!")
            
            # List folders
            print("   📁 Checking folder structure...")
            blobs = list(client.list_blobs(bucket, delimiter='/'))
            
            folders = set()
            for blob in blobs:
                if blob.name.endswith('/'):
                    folders.add(blob.name.rstrip('/'))
            
            print(f"   Found folders: {list(folders) if folders else 'None'}")
            
            # Check if our required folders exist
            required_folders = ['bronze', 'silver', 'gold', 'logs', 'temp']
            missing_folders = []
            
            for folder in required_folders:
                folder_blob = bucket.blob(f"{folder}/")
                if not folder_blob.exists():
                    missing_folders.append(folder)
            
            if missing_folders:
                print(f"   ⚠️  Missing folders: {missing_folders}")
                print("   You may need to create these folders in your GCS bucket")
            else:
                print("   ✅ All required folders are present!")
                
        else:
            print(f"   ❌ Bucket '{config.bucket_name}' not found or not accessible")
            print("   Please check your bucket name and permissions")
            return False
        
        print("   🎉 GCS connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"   ❌ GCS connection test failed: {e}")
        print("   Please check your configuration and credentials")
        return False

if __name__ == "__main__":
    success = test_gcs_connection()
    if success:
        print("\n✅ GCS is ready for use!")
    else:
        print("\n❌ GCS setup needs attention!")
