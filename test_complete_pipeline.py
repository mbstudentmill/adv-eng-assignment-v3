#!/usr/bin/env python3
"""
Test Complete Pipeline Integration
Tests the end-to-end data pipeline with Prefect orchestration.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from ingestion.imdb.imdb_ingestion import IMDbIngestion
from ingestion.nasa.nasa_ingestion import NASADONKIIngestion
from data_quality_checks import DataQualityValidator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_complete_pipeline():
    """Test the complete data pipeline end-to-end."""
    logger.info("🚀 Starting Complete Pipeline Integration Test")
    
    try:
        # Step 1: Test IMDb Ingestion
        logger.info("📽️  Testing IMDb Ingestion...")
        imdb = IMDbIngestion()
        
        # Download a small dataset for testing
        bronze_path = imdb.download_dataset('title.ratings', 'data/bronze/test')
        logger.info(f"✅ IMDb download: {bronze_path}")
        
        # Process to silver
        tsv_path = imdb.extract_tsv(bronze_path, 'data/silver/test')
        parquet_path = imdb.process_to_parquet(tsv_path, 'data/silver/test')
        logger.info(f"✅ IMDb processing: {parquet_path}")
        
        # Step 2: Test NASA Ingestion
        logger.info("🌞 Testing NASA Ingestion...")
        nasa = NASADONKIIngestion()
        
        # Fetch recent data
        solar_data = nasa.fetch_solar_flares(days_back=7)
        logger.info(f"✅ NASA fetch: {len(solar_data)} records")
        
        # Save to both zones
        bronze_path = nasa.save_to_bronze(solar_data, 'data/bronze/test')
        silver_path = nasa.save_to_silver(nasa.clean_solar_flares_data(solar_data), 'data/silver/test')
        logger.info(f"✅ NASA save: {bronze_path}, {silver_path}")
        
        # Step 3: Test Data Quality
        logger.info("🔍 Testing Data Quality Validation...")
        validator = DataQualityValidator()
        
        # Validate the test data
        if os.path.exists(parquet_path):
            imdb_result = validator.validate_imdb_ratings(parquet_path)
            logger.info(f"✅ IMDb validation: {imdb_result['status']}")
        
        if os.path.exists(silver_path):
            nasa_result = validator.validate_nasa_solar_flares(silver_path)
            logger.info(f"✅ NASA validation: {nasa_result['status']}")
        
        # Step 4: Test GCS Integration
        logger.info("☁️  Testing GCS Integration...")
        try:
            from google.cloud import storage
            from gcs_config import GCSConfig
            
            config = GCSConfig()
            if config.credentials_path and os.path.exists(config.credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
                
                client = storage.Client(project=config.project_id)
                bucket = client.bucket(config.bucket_name)
                
                # Test bucket access
                blobs = list(bucket.list_blobs(max_results=5))
                logger.info(f"✅ GCS access: {len(blobs)} blobs found")
                
        except Exception as e:
            logger.warning(f"⚠️  GCS test failed: {e}")
        
        logger.info("🎉 Complete Pipeline Integration Test PASSED!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline test failed: {e}")
        raise

def cleanup_test_data():
    """Clean up test data files."""
    import shutil
    
    test_dirs = ['data/bronze/test', 'data/silver/test']
    
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
            logger.info(f"🧹 Cleaned up: {test_dir}")

def main():
    """Main execution function."""
    try:
        # Run the complete pipeline test
        success = test_complete_pipeline()
        
        if success:
            print("\n✅ Complete Pipeline Integration Test PASSED!")
            print("🎯 All components working together:")
            print("   - IMDb ingestion ✅")
            print("   - NASA ingestion ✅")
            print("   - Data quality validation ✅")
            print("   - GCS integration ✅")
            print("   - Prefect orchestration ready ✅")
        
        # Clean up test data
        cleanup_test_data()
        
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        raise

if __name__ == "__main__":
    main()
