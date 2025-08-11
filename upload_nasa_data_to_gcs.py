#!/usr/bin/env python3
"""
Upload NASA Data to GCS
Uploads the actual processed NASA data files to GCS bronze and silver zones.
"""

import os
from google.cloud import storage
from gcs_config import GCSConfig

def upload_nasa_data_to_gcs():
    """Upload NASA data files to GCS zones."""
    config = GCSConfig()
    
    # Set credentials
    if config.credentials_path and os.path.exists(config.credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
        print(f"🔐 Using credentials from: {config.credentials_path}")
    else:
        raise ValueError(f"Credentials file not found at: {config.credentials_path}")
    
    client = storage.Client(project=config.project_id)
    bucket = client.bucket(config.bucket_name)
    
    print(f"🚀 Uploading NASA data to GCS bucket: {config.bucket_name}")
    
    # Upload bronze zone files (raw data)
    bronze_files = [
        ('data/bronze/nasa_solar_flares_20250810_174507.json', f"{config.bronze_path}/nasa/nasa_solar_flares_20250810_174507.json")
    ]
    
    for local_path, gcs_path in bronze_files:
        if os.path.exists(local_path):
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"✅ Uploaded {local_path} to bronze zone: {gcs_path}")
        else:
            print(f"⚠️  File not found: {local_path}")
    
    # Upload silver zone files (processed data)
    silver_files = [
        ('data/silver/nasa_solar_flares_20250810_174507.parquet', f"{config.silver_path}/nasa/nasa_solar_flares_20250810_174507.parquet")
    ]
    
    for local_path, gcs_path in silver_files:
        if os.path.exists(local_path):
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"✅ Uploaded {local_path} to silver zone: {gcs_path}")
        else:
            print(f"⚠️  File not found: {local_path}")
    
    print(f"\n📊 Summary of NASA data uploaded to GCS:")
    print(f"   Bronze zone: {len(bronze_files)} files")
    print(f"   Silver zone: {len(silver_files)} files")
    
    return {'bronze': bronze_files, 'silver': silver_files}

if __name__ == "__main__":
    try:
        result = upload_nasa_data_to_gcs()
        print("✅ NASA data upload completed successfully!")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        raise
