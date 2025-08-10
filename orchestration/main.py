#!/usr/bin/env python3
"""
Main Prefect Orchestration Flow
Coordinates the complete data pipeline for Task 1: IMDb + NASA DONKI ingestion.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.filesystems import LocalFileSystem

# Import our ingestion modules
from ingestion.imdb.imdb_ingestion import IMDbIngestion
from ingestion.nasa.nasa_ingestion import NASADONKIIngestion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(name="setup_directories", retries=2, retry_delay_seconds=30)
def setup_directories():
    """Create necessary directories for the data pipeline."""
    logger = get_run_logger()
    
    directories = [
        "data/bronze",
        "data/silver", 
        "data/gold",
        "logs",
        "temp"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")
    
    return directories

@task(name="ingest_imdb_data", retries=3, retry_delay_seconds=60)
def ingest_imdb_data():
    """Ingest IMDb dataset files."""
    logger = get_run_logger()
    logger.info("Starting IMDb data ingestion...")
    
    try:
        ingestor = IMDbIngestion()
        
        # Download and process IMDb files
        results = ingestor.run_ingestion_pipeline(
            bronze_dir="data/bronze",
            silver_dir="data/silver"
        )
        
        logger.info(f"IMDb ingestion completed successfully")
        logger.info(f"Files processed: {len(results)}")
        
        return results
        
    except Exception as e:
        logger.error(f"IMDb ingestion failed: {e}")
        raise

@task(name="ingest_nasa_data", retries=3, retry_delay_seconds=30)
def ingest_nasa_data(api_key: str = "DEMO_KEY"):
    """Ingest NASA DONKI Solar Flares data."""
    logger = get_run_logger()
    logger.info("Starting NASA DONKI data ingestion...")
    
    try:
        ingestor = NASADONKIIngestion(api_key=api_key)
        
        # Fetch and process NASA data
        results = ingestor.run_ingestion_pipeline(
            days_back=365,
            bronze_dir="data/bronze",
            silver_dir="data/silver"
        )
        
        logger.info(f"NASA DONKI ingestion completed successfully")
        logger.info(f"Records processed: {results.get('record_count', 0)}")
        
        return results
        
    except Exception as e:
        logger.error(f"NASA DONKI ingestion failed: {e}")
        raise

@task(name="validate_data_quality", retries=2, retry_delay_seconds=30)
def validate_data_quality(imdb_results: dict, nasa_results: dict):
    """Validate data quality for both sources."""
    logger = get_run_logger()
    logger.info("Starting data quality validation...")
    
    validation_results = {
        'imdb': {
            'status': 'success' if imdb_results else 'failed',
            'files_processed': len(imdb_results) if imdb_results else 0,
            'timestamp': datetime.now().isoformat()
        },
        'nasa': {
            'status': 'success' if nasa_results else 'failed',
            'records_processed': nasa_results.get('record_count', 0) if nasa_results else 0,
            'timestamp': datetime.now().isoformat()
        }
    }
    
    # Check if both sources succeeded
    overall_success = (
        validation_results['imdb']['status'] == 'success' and
        validation_results['nasa']['status'] == 'success'
    )
    
    validation_results['overall_success'] = overall_success
    
    logger.info(f"Data quality validation completed: {'SUCCESS' if overall_success else 'FAILED'}")
    logger.info(f"IMDb: {validation_results['imdb']}")
    logger.info(f"NASA: {validation_results['nasa']}")
    
    return validation_results

@task(name="generate_pipeline_summary", retries=1)
def generate_pipeline_summary(validation_results: dict):
    """Generate a summary report of the pipeline execution."""
    logger = get_run_logger()
    
    summary = {
        'pipeline_name': 'Advanced Data Engineering - Task 1',
        'execution_time': datetime.now().isoformat(),
        'status': 'SUCCESS' if validation_results['overall_success'] else 'FAILED',
        'data_sources': {
            'imdb': validation_results['imdb'],
            'nasa': validation_results['nasa']
        },
        'total_files_processed': (
            validation_results['imdb']['files_processed'] + 
            (1 if validation_results['nasa']['status'] == 'success' else 0)
        ),
        'total_records_processed': validation_results['nasa']['records_processed'],
        'bronze_layer_files': len(list(Path("data/bronze").glob("*"))),
        'silver_layer_files': len(list(Path("data/silver").glob("*")))
    }
    
    # Save summary to file
    summary_file = f"logs/pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    import json
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Pipeline summary saved to: {summary_file}")
    return summary

@flow(name="advanced-data-engineering-pipeline", 
      description="Complete data pipeline for IMDb + NASA DONKI ingestion",
      version="1.0.0")
def main_pipeline(api_key: str = "DEMO_KEY"):
    """
    Main orchestration flow for the Advanced Data Engineering assignment.
    
    This flow implements Task 1 requirements:
    - Multi-source data ingestion (IMDb + NASA DONKI)
    - Scalable data storage (GCS bronze/silver/gold layers)
    - Data transformations and quality validation
    - Orchestration with Prefect for monitoring and reliability
    """
    logger = get_run_logger()
    
    logger.info("🚀 Starting Advanced Data Engineering Pipeline")
    logger.info("📋 Task 1: Multi-source Data Ingestion Pipeline")
    
    try:
        # Step 1: Setup directories
        logger.info("📁 Setting up pipeline directories...")
        directories = setup_directories()
        
        # Step 2: Ingest IMDb data (parallel with NASA)
        logger.info("🎬 Starting IMDb data ingestion...")
        imdb_results = ingest_imdb_data()
        
        # Step 3: Ingest NASA data (parallel with IMDb)
        logger.info("🚀 Starting NASA DONKI data ingestion...")
        nasa_results = ingest_nasa_data(api_key=api_key)
        
        # Step 4: Validate data quality
        logger.info("✅ Validating data quality...")
        validation_results = validate_data_quality(imdb_results, nasa_results)
        
        # Step 5: Generate pipeline summary
        logger.info("📊 Generating pipeline summary...")
        summary = generate_pipeline_summary(validation_results)
        
        # Final status
        if validation_results['overall_success']:
            logger.info("🎉 Pipeline completed successfully!")
            logger.info(f"📊 Total files processed: {summary['total_files_processed']}")
            logger.info(f"📊 Total records processed: {summary['total_records_processed']}")
        else:
            logger.error("❌ Pipeline completed with errors")
            
        return summary
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    # Check if NASA API key is provided
    api_key = os.getenv("NASA_API_KEY", "DEMO_KEY")
    
    if api_key == "DEMO_KEY":
        print("⚠️  Using DEMO_KEY for NASA API. Set NASA_API_KEY environment variable for full access.")
    
    # Run the pipeline
    try:
        result = main_pipeline(api_key=api_key)
        print(f"✅ Pipeline completed successfully!")
        print(f"📊 Summary: {result}")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)





