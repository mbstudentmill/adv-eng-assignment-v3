#!/usr/bin/env python3
"""
Simple BigQuery Connection Test
Tests basic connectivity and dataset creation.
"""

import os
from pathlib import Path
from google.cloud import bigquery
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_bigquery_connection():
    """Test basic BigQuery connectivity."""
    try:
        # Set credentials path
        credentials_path = Path(__file__).parent / "gcp-credentials.json"
        if credentials_path.exists():
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_path)
            logger.info(f"Using credentials from: {credentials_path}")
        else:
            logger.warning("gcp-credentials.json not found, using default authentication")
        
        # Test different project IDs
        project_ids = ["your-gcp-project-id", "your-gcp-project-id-123", "your-gcp-project-id-456"]
        
        for project_id in project_ids:
            try:
                logger.info(f"Testing project ID: {project_id}")
                client = bigquery.Client(project=project_id)
                
                # List datasets
                datasets = list(client.list_datasets())
                logger.info(f"✅ Project {project_id}: Found {len(datasets)} datasets")
                
                # Try to create a test dataset
                dataset_id = f"{project_id}.test_dataset"
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"  # Set location
                
                try:
                    dataset = client.create_dataset(dataset, timeout=30)
                    logger.info(f"✅ Successfully created test dataset in {project_id}")
                    
                    # Clean up - delete test dataset
                    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
                    logger.info(f"✅ Cleaned up test dataset from {project_id}")
                    
                    # Now try to create the real dataset
                    real_dataset_id = f"{project_id}.imdb_warehouse"
                    real_dataset = bigquery.Dataset(real_dataset_id)
                    real_dataset.location = "US"
                    real_dataset.description = "IMDb Data Warehouse for Advanced Data Engineering Assignment"
                    
                    real_dataset = client.create_dataset(real_dataset, timeout=30)
                    logger.info(f"✅ Successfully created imdb_warehouse dataset in {project_id}")
                    
                    return project_id, client
                    
                except Exception as e:
                    logger.warning(f"⚠️  Could not create dataset in {project_id}: {e}")
                    continue
                    
            except Exception as e:
                logger.warning(f"⚠️  Project {project_id} failed: {e}")
                continue
        
        logger.error("❌ No project ID worked")
        return None, None
        
    except Exception as e:
        logger.error(f"❌ BigQuery connection test failed: {e}")
        return None, None

def main():
    """Main test function."""
    logger.info("🔧 Testing BigQuery Connection...")
    
    project_id, client = test_bigquery_connection()
    
    if project_id and client:
        logger.info(f"✅ BigQuery connection successful for project: {project_id}")
        logger.info("📋 Next steps:")
        logger.info("  1. Update the project ID in your DDL scripts")
        logger.info("  2. Run the Task 2 execution script")
        logger.info("  3. Complete the data warehouse setup")
    else:
        logger.error("❌ BigQuery connection failed")
        logger.info("📋 Troubleshooting steps:")
        logger.info("  1. Check your GCP credentials")
        logger.info("  2. Verify the project ID exists")
        logger.info("  3. Ensure BigQuery API is enabled")

if __name__ == "__main__":
    main()
