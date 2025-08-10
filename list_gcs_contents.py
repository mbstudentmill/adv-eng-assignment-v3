#!/usr/bin/env python3
"""
List GCS Bucket Contents
Shows all files and folders in the GCS bucket to verify IMDb data placement.
"""

import os
from google.cloud import storage
from gcs_config import get_gcs_config

def list_gcs_contents():
    """List all contents of the GCS bucket."""
    
    # Get GCS configuration
    config = get_gcs_config()
    
    # Set credentials explicitly
    if config.credentials_path and os.path.exists(config.credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
        print(f"🔐 Using credentials from: {config.credentials_path}")
    else:
        raise ValueError(f"Credentials file not found at: {config.credentials_path}")
    
    client = storage.Client(project=config.project_id)
    bucket = client.bucket(config.bucket_name)
    
    print(f"\n📦 Listing contents of bucket: {config.bucket_name}")
    print(f"   Project: {config.project_id}")
    print(f"   Region: {config.region}")
    print("=" * 60)
    
    # List all blobs in the bucket
    blobs = list(bucket.list_blobs())
    
    if not blobs:
        print("❌ No files found in bucket!")
        return
    
    # Group files by zone
    zones = {}
    for blob in blobs:
        # Extract zone from path (e.g., "bronze/name.basics.tsv.gz" -> "bronze")
        path_parts = blob.name.split('/')
        if len(path_parts) >= 2:
            zone = path_parts[0]
            filename = '/'.join(path_parts[1:])
            
            if zone not in zones:
                zones[zone] = []
            zones[zone].append({
                'name': filename,
                'size': blob.size,
                'updated': blob.updated,
                'path': blob.name
            })
    
    # Display files by zone
    total_files = 0
    for zone in ['bronze', 'silver', 'gold', 'logs', 'temp']:
        if zone in zones:
            print(f"\n🏆 {zone.upper()} ZONE:")
            print("-" * 40)
            
            zone_files = zones[zone]
            zone_files.sort(key=lambda x: x['name'])
            
            for file_info in zone_files:
                size_mb = file_info['size'] / (1024 * 1024) if file_info['size'] > 0 else 0
                print(f"   📄 {file_info['name']}")
                print(f"      Size: {size_mb:.2f} MB")
                print(f"      Updated: {file_info['updated']}")
                print(f"      Path: gs://{config.bucket_name}/{file_info['path']}")
                print()
                total_files += 1
        else:
            print(f"\n🏆 {zone.upper()} ZONE: (Empty)")
            print("-" * 40)
            print("   No files found")
    
    print("=" * 60)
    print(f"📊 Total files in bucket: {total_files}")
    print(f"🎯 Zones with files: {len([z for z in zones.keys() if zones[z]])}")
    
    # Show bucket statistics
    bucket.reload()
    print(f"💰 Bucket storage class: {bucket.storage_class}")
    print(f"📅 Bucket created: {bucket.time_created}")
    print(f"🔄 Bucket updated: {bucket.updated}")

def main():
    """Main execution function."""
    try:
        list_gcs_contents()
        print(f"\n✅ GCS bucket contents listed successfully!")
        
    except Exception as e:
        print(f"❌ Failed to list GCS contents: {e}")
        raise

if __name__ == "__main__":
    main()
