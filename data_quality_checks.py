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
    
    def validate_dataset_completeness(self, file_path: str, expected_columns: list) -> dict:
        """Validate dataset completeness against expected schema."""
        logger.info(f"Validating dataset completeness: {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            
            # Check column presence
            missing_columns = set(expected_columns) - set(df.columns)
            extra_columns = set(df.columns) - set(expected_columns)
            
            # Check data types
            column_types = df.dtypes.to_dict()
            
            # Check for empty datasets
            is_empty = len(df) == 0
            
            validation_result = {
                'dataset': 'Dataset Completeness',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'schema_validation': {
                    'expected_columns': expected_columns,
                    'actual_columns': list(df.columns),
                    'missing_columns': list(missing_columns),
                    'extra_columns': list(extra_columns),
                    'column_types': {col: str(dtype) for col, dtype in column_types.items()}
                },
                'data_validation': {
                    'is_empty': is_empty,
                    'total_rows': len(df)
                },
                'status': 'PASSED' if not missing_columns and not is_empty else 'FAILED'
            }
            
            logger.info(f"Completeness validation: {len(missing_columns)} missing columns, empty: {is_empty}")
            return validation_result
            
        except Exception as e:
            logger.error(f"Completeness validation failed: {e}")
            return {
                'dataset': 'Dataset Completeness',
                'file_path': file_path,
                'validation_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'status': 'ERROR'
            }
    
    def run_all_validations(self) -> dict:
        """Run all available validations on datasets."""
        logger.info("🚀 Starting comprehensive data quality validation...")
        
        all_results = {}
        
        # Example validation calls (modify paths as needed)
        # if os.path.exists("data/silver/imdb_ratings.parquet"):
        #     all_results['imdb_ratings'] = self.validate_imdb_ratings("data/silver/imdb_ratings.parquet")
        # 
        # if os.path.exists("data/silver/nasa_solar_flares.parquet"):
        #     all_results['nasa_solar_flares'] = self.validate_nasa_solar_flares("data/silver/nasa_solar_flares.parquet")
        
        logger.info(f"✅ Validation completed: {len(all_results)} datasets processed")
        return all_results
    
    def save_validation_results(self, results: dict):
        """Save validation results to JSON file."""
        try:
            output_path = "logs/data_quality_validation.json"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"✅ Validation results saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save validation results: {e}")

def main():
    """Main execution function."""
    try:
        validator = DataQualityValidator()
        
        print("🔍 Starting Data Quality Validation...")
        print("=" * 50)
        
        # Run validations
        results = validator.run_all_validations()
        
        if results:
            # Save results
            validator.save_validation_results(results)
            
            # Display summary
            print("\n📊 Validation Results Summary:")
            print("=" * 30)
            
            for dataset_name, result in results.items():
                status = "✅ PASS" if result['status'] == 'PASSED' else "❌ FAIL"
                print(f"{status} {dataset_name}")
                
                if result['status'] == 'PASSED':
                    stats = result.get('basic_stats', {})
                    print(f"   📊 Rows: {stats.get('total_rows', 'N/A')}")
                    print(f"   📋 Columns: {stats.get('total_columns', 'N/A')}")
                else:
                    print(f"   ❌ Error: {result.get('error', 'Unknown error')}")
                print()
            
            successful_validations = sum(1 for r in results.values() if r['status'] == 'PASSED')
            total_validations = len(results)
            
            print(f"🎯 Overall Results: {successful_validations}/{total_validations} validations passed")
            
            if successful_validations == total_validations:
                print("🎉 All data quality validations passed!")
            else:
                print("⚠️  Some validations failed. Check the logs for details.")
        else:
            print("⚠️  No datasets found for validation")
            print("📋 Make sure to run data ingestion first")
        
    except Exception as e:
        print(f"❌ Data quality validation failed: {e}")
        raise

if __name__ == "__main__":
    main()
