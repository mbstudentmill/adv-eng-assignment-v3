#!/usr/bin/env python3
"""
Data Quality Checks
Implements data quality validation for ingested datasets using pandas.
"""

import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Validates data quality for ingested datasets."""
    
    def __init__(self):
        self.validation_results = {}
        
    def validate_imdb_ratings(self, file_path: str) -> dict:
        """Validate IMDb ratings dataset."""
        logger.info(f"Validating IMDb ratings: {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            
            # Basic statistics
            total_rows = len(df)
            total_columns = len(df.columns)
            
            # Data quality checks
            null_counts = df.isnull().sum().to_dict()
            duplicate_rows = df.duplicated().sum()
            
            # Field-specific validations
            avg_rating = df['averageRating'].mean() if 'averageRating' in df.columns else None
            num_votes_range = (df['numVotes'].min(), df['numVotes'].max()) if 'numVotes' in df.columns else (None, None)
            
            # Validation results
            validation_result = {
                'dataset': 'IMDb Ratings',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'basic_stats': {
                    'total_rows': total_rows,
                    'total_columns': total_columns,
                    'columns': list(df.columns)
                },
                'data_quality': {
                    'null_counts': null_counts,
                    'duplicate_rows': duplicate_rows,
                    'avg_rating': avg_rating,
                    'num_votes_range': num_votes_range
                },
                'status': 'PASSED' if total_rows > 0 and duplicate_rows == 0 else 'FAILED'
            }
            
            logger.info(f"IMDb validation completed: {total_rows} rows, {duplicate_rows} duplicates")
            return validation_result
            
        except Exception as e:
            logger.error(f"IMDb validation failed: {e}")
            return {
                'dataset': 'IMDb Ratings',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'status': 'ERROR'
            }
    
    def validate_nasa_solar_flares(self, file_path: str) -> dict:
        """Validate NASA solar flares dataset."""
        logger.info(f"Validating NASA solar flares: {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            
            # Basic statistics
            total_rows = len(df)
            total_columns = len(df.columns)
            
            # Data quality checks - handle numpy arrays in instrument column
            null_counts = df.isnull().sum().to_dict()
            
            # For duplicate checking, exclude columns with numpy arrays
            columns_for_duplicates = [col for col in df.columns if col != 'instrument']
            duplicate_rows = df[columns_for_duplicates].duplicated().sum()
            
            # Field-specific validations
            event_ids = df['event_id'].nunique() if 'event_id' in df.columns else 0
            classes = df['class'].nunique() if 'class' in df.columns else 0
            
            # Additional validations
            valid_classes = df['class'].value_counts().to_dict() if 'class' in df.columns else {}
            source_locations = df['source_location'].nunique() if 'source_location' in df.columns else 0
            
            # Validation results
            validation_result = {
                'dataset': 'NASA Solar Flares',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'basic_stats': {
                    'total_rows': total_rows,
                    'total_columns': total_columns,
                    'columns': list(df.columns)
                },
                'data_quality': {
                    'null_counts': null_counts,
                    'duplicate_rows': duplicate_rows,
                    'unique_event_ids': event_ids,
                    'unique_classes': classes,
                    'class_distribution': valid_classes,
                    'unique_source_locations': source_locations
                },
                'status': 'PASSED' if total_rows > 0 and duplicate_rows == 0 else 'FAILED'
            }
            
            logger.info(f"NASA validation completed: {total_rows} rows, {duplicate_rows} duplicates")
            return validation_result
            
        except Exception as e:
            logger.error(f"NASA validation failed: {e}")
            return {
                'dataset': 'NASA Solar Flares',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'status': 'ERROR'
            }
    
    def run_all_validations(self) -> dict:
        """Run validation on all available datasets."""
        logger.info("Starting comprehensive data quality validation...")
        
        validation_results = {}
        
        # Validate IMDb data
        imdb_silver_path = "data/silver/title.ratings.parquet"
        if os.path.exists(imdb_silver_path):
            validation_results['imdb_ratings'] = self.validate_imdb_ratings(imdb_silver_path)
        
        # Validate NASA data
        nasa_silver_path = "data/silver/nasa_solar_flares_20250810_174507.parquet"
        if os.path.exists(nasa_silver_path):
            validation_results['nasa_solar_flares'] = self.validate_nasa_solar_flares(nasa_silver_path)
        
        # Save validation results
        self.save_validation_results(validation_results)
        
        logger.info("Data quality validation completed")
        return validation_results
    
    def save_validation_results(self, results: dict):
        """Save validation results to logs zone."""
        logs_dir = "data/logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(logs_dir, f"data_quality_validation_{timestamp}.json")
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Validation results saved to: {output_path}")
        
        # Also save to GCS logs zone
        try:
            from google.cloud import storage
            from gcs_config import GCSConfig
            
            config = GCSConfig()
            if config.credentials_path and os.path.exists(config.credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials_path
                
                client = storage.Client(project=config.project_id)
                bucket = client.bucket(config.bucket_name)
                
                gcs_path = f"{config.logs_path}/data_quality_validation_{timestamp}.json"
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(output_path)
                
                logger.info(f"Validation results uploaded to GCS: {gcs_path}")
                
        except Exception as e:
            logger.warning(f"Failed to upload validation results to GCS: {e}")

def main():
    """Main execution function."""
    validator = DataQualityValidator()
    
    try:
        results = validator.run_all_validations()
        
        print("\n📊 Data Quality Validation Results:")
        print("=" * 50)
        
        for dataset_name, result in results.items():
            print(f"\n🔍 {result['dataset']}:")
            print(f"   Status: {result['status']}")
            if 'basic_stats' in result:
                print(f"   Rows: {result['basic_stats']['total_rows']}")
                print(f"   Columns: {result['basic_stats']['total_columns']}")
            if 'error' in result:
                print(f"   Error: {result['error']}")
        
        print(f"\n✅ Validation completed. Results saved to data/logs/")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        raise

if __name__ == "__main__":
    main()
