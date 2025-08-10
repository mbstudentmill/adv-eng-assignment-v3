#!/usr/bin/env python3
"""
IMDb Data Ingestion Script
Downloads and processes IMDb dataset files for the data pipeline.
"""

import os
import gzip
import requests
import pandas as pd
from pathlib import Path
from typing import List, Dict
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IMDbIngestion:
    """Handles ingestion of IMDb dataset files."""
    
    def __init__(self, base_url: str = "https://datasets.imdbws.com/"):
        self.base_url = base_url
        self.datasets = {
            'name.basics': 'name.basics.tsv.gz',
            'title.akas': 'title.akas.tsv.gz', 
            'title.basics': 'title.basics.tsv.gz',
            'title.crew': 'title.crew.tsv.gz',
            'title.episode': 'title.episode.tsv.gz',
            'title.principals': 'title.principals.tsv.gz',
            'title.ratings': 'title.ratings.tsv.gz'
        }
        
    def download_dataset(self, dataset_name: str, output_dir: str = "data/bronze") -> str:
        """Download a single IMDb dataset file."""
        if dataset_name not in self.datasets:
            raise ValueError(f"Unknown dataset: {dataset_name}")
            
        filename = self.datasets[dataset_name]
        url = f"{self.base_url}{filename}"
        output_path = os.path.join(output_dir, filename)
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Downloading {dataset_name} from {url}")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as f, tqdm(
                desc=f"Downloading {dataset_name}",
                total=total_size,
                unit='B',
                unit_scale=True
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
                        
            logger.info(f"Successfully downloaded {dataset_name} to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to download {dataset_name}: {e}")
            raise
    
    def download_all_datasets(self, output_dir: str = "data/bronze") -> Dict[str, str]:
        """Download all IMDb dataset files."""
        downloaded_files = {}
        
        logger.info("Starting download of all IMDb datasets...")
        
        for dataset_name in self.datasets.keys():
            try:
                file_path = self.download_dataset(dataset_name, output_dir)
                downloaded_files[dataset_name] = file_path
            except Exception as e:
                logger.error(f"Failed to download {dataset_name}: {e}")
                continue
                
        logger.info(f"Downloaded {len(downloaded_files)} out of {len(self.datasets)} datasets")
        return downloaded_files
    
    def extract_tsv(self, gz_file_path: str, output_dir: str = "data/silver") -> str:
        """Extract and convert gzipped TSV to readable format."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract filename without .gz extension
        base_name = os.path.basename(gz_file_path).replace('.gz', '')
        output_path = os.path.join(output_dir, base_name)
        
        logger.info(f"Extracting {gz_file_path} to {output_path}")
        
        try:
            with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
                with open(output_path, 'w', encoding='utf-8') as out_file:
                    for line in gz_file:
                        out_file.write(line)
                        
            logger.info(f"Successfully extracted {gz_file_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to extract {gz_file_path}: {e}")
            raise
    
    def process_to_parquet(self, tsv_file_path: str, output_dir: str = "data/silver") -> str:
        """Convert TSV file to Parquet format for better performance."""
        os.makedirs(output_dir, exist_ok=True)
        
        base_name = os.path.basename(tsv_file_path).replace('.tsv', '')
        output_path = os.path.join(output_dir, f"{base_name}.parquet")
        
        logger.info(f"Converting {tsv_file_path} to Parquet")
        
        try:
            # Read TSV in chunks to handle large files
            chunk_size = 100000
            chunks = []
            
            for chunk in pd.read_csv(tsv_file_path, sep='\t', chunksize=chunk_size, low_memory=False):
                chunks.append(chunk)
            
            # Combine chunks and save as Parquet
            df = pd.concat(chunks, ignore_index=True)
            df.to_parquet(output_path, index=False)
            
            logger.info(f"Successfully converted to {output_path} with {len(df)} rows")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to convert {tsv_file_path}: {e}")
            raise
    
    def run_ingestion_pipeline(self, bronze_dir: str = "data/bronze", silver_dir: str = "data/silver") -> Dict[str, str]:
        """Run the complete IMDb ingestion pipeline."""
        logger.info("Starting IMDb ingestion pipeline...")
        
        try:
            # Download all datasets to bronze layer
            downloaded_files = self.download_all_datasets(bronze_dir)
            
            # Process key tables to silver layer (Parquet)
            key_tables = ['title.basics', 'title.ratings', 'title.principals', 'name.basics', 'title.akas']
            processed_files = {}
            
            for table in key_tables:
                if table in downloaded_files:
                    gz_path = downloaded_files[table]
                    
                    # Extract TSV
                    tsv_path = self.extract_tsv(gz_path, silver_dir)
                    
                    # Convert to Parquet
                    parquet_path = self.process_to_parquet(tsv_path, silver_dir)
                    
                    processed_files[table] = {
                        'bronze': gz_path,
                        'tsv': tsv_path,
                        'silver': parquet_path
                    }
                    
                    logger.info(f"Processed {table}: {gz_path} -> {tsv_path} -> {parquet_path}")
            
            logger.info("IMDb ingestion pipeline completed successfully")
            return processed_files
            
        except Exception as e:
            logger.error(f"IMDb ingestion pipeline failed: {e}")
            raise

def main():
    """Main execution function."""
    ingestor = IMDbIngestion()
    
    # Download all datasets to bronze layer
    logger.info("=== Starting IMDb Data Ingestion ===")
    
    try:
        # Download all files to bronze layer
        downloaded_files = ingestor.download_all_datasets("data/bronze")
        
        # Process key tables to silver layer (Parquet)
        key_tables = ['title.basics', 'title.ratings', 'title.principals', 'name.basics', 'title.akas']
        
        for table in key_tables:
            if table in downloaded_files:
                gz_path = downloaded_files[table]
                
                # Extract TSV
                tsv_path = ingestor.extract_tsv(gz_path, "data/silver")
                
                # Convert to Parquet
                parquet_path = ingestor.process_to_parquet(tsv_path, "data/silver")
                
                logger.info(f"Processed {table}: {gz_path} -> {tsv_path} -> {parquet_path}")
        
        logger.info("=== IMDb Data Ingestion Complete ===")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()



