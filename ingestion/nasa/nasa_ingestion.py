#!/usr/bin/env python3
"""
NASA DONKI API Ingestion Script
Fetches Solar Flares data from NASA's DONKI API for Task 1 multi-source requirement.
"""

import os
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from tqdm import tqdm
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NASADONKIIngestion:
    """Handles ingestion of NASA DONKI Solar Flares data."""
    
    def __init__(self, api_key: str = "DEMO_KEY"):
        self.api_key = api_key
        self.base_url = "https://api.nasa.gov/DONKI"
        self.endpoint = "FLR"  # Solar Flares
        
    def fetch_solar_flares(self, start_date: str = None, end_date: str = None, 
                          days_back: int = 365) -> List[Dict]:
        """
        Fetch Solar Flares data from NASA DONKI API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            days_back: Number of days to go back from today (if dates not provided)
        
        Returns:
            List of solar flare records
        """
        if not start_date or not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/{self.endpoint}"
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'api_key': self.api_key
        }
        
        logger.info(f"Fetching solar flares from {start_date} to {end_date}")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not isinstance(data, list):
                logger.warning(f"Unexpected response format: {type(data)}")
                return []
            
            logger.info(f"Successfully fetched {len(data)} solar flare records")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return []
    
    def clean_solar_flares_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Clean and structure the raw solar flares data.
        
        Args:
            raw_data: Raw API response data
            
        Returns:
            Cleaned pandas DataFrame
        """
        if not raw_data:
            logger.warning("No data to clean")
            return pd.DataFrame()
        
        # Extract key fields for Task 1 evidence
        cleaned_records = []
        
        for record in raw_data:
            cleaned_record = {
                'event_id': record.get('flrID', ''),
                'start_time': record.get('beginTime', ''),
                'peak_time': record.get('peakTime', ''),
                'end_time': record.get('endTime', ''),
                'class': record.get('classType', ''),
                'source_location': record.get('sourceLocation', ''),
                'region_number': record.get('activeRegionNum', ''),
                'instrument': record.get('instruments', []),
                'raw_data': json.dumps(record)  # Keep raw JSON for bronze layer
            }
            cleaned_records.append(cleaned_record)
        
        df = pd.DataFrame(cleaned_records)
        
        # Data quality checks
        logger.info(f"Data quality summary:")
        logger.info(f"  - Total records: {len(df)}")
        logger.info(f"  - Records with event_id: {df['event_id'].notna().sum()}")
        logger.info(f"  - Records with start_time: {df['start_time'].notna().sum()}")
        logger.info(f"  - Records with class: {df['class'].notna().sum()}")
        
        return df
    
    def save_to_bronze(self, raw_data: List[Dict], output_dir: str = "data/bronze") -> str:
        """Save raw API response to bronze layer."""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nasa_solar_flares_{timestamp}.json"
        output_path = os.path.join(output_dir, filename)
        
        try:
            with open(output_path, 'w') as f:
                json.dump(raw_data, f, indent=2)
            
            logger.info(f"Raw data saved to bronze layer: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save raw data: {e}")
            raise
    
    def save_to_silver(self, df: pd.DataFrame, output_dir: str = "data/silver") -> str:
        """Save cleaned data to silver layer as Parquet."""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nasa_solar_flares_{timestamp}.parquet"
        output_path = os.path.join(output_dir, filename)
        
        try:
            df.to_parquet(output_path, index=False)
            logger.info(f"Cleaned data saved to silver layer: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save cleaned data: {e}")
            raise
    
    def run_ingestion_pipeline(self, days_back: int = 365, 
                              bronze_dir: str = "data/bronze",
                              silver_dir: str = "data/silver") -> Dict[str, str]:
        """
        Run the complete NASA ingestion pipeline.
        
        Args:
            days_back: Number of days to fetch
            bronze_dir: Bronze layer directory
            silver_dir: Silver layer directory
            
        Returns:
            Dictionary with file paths
        """
        logger.info("=== Starting NASA DONKI Ingestion Pipeline ===")
        
        try:
            # Step 1: Fetch data from API
            raw_data = self.fetch_solar_flares(days_back=days_back)
            
            if not raw_data:
                logger.error("No data fetched from API")
                return {}
            
            # Step 2: Save raw data to bronze layer
            bronze_path = self.save_to_bronze(raw_data, bronze_dir)
            
            # Step 3: Clean and structure data
            cleaned_df = self.clean_solar_flares_data(raw_data)
            
            if cleaned_df.empty:
                logger.error("Data cleaning failed")
                return {}
            
            # Step 4: Save cleaned data to silver layer
            silver_path = self.save_to_silver(cleaned_df, silver_dir)
            
            logger.info("=== NASA DONKI Ingestion Pipeline Complete ===")
            
            return {
                'bronze': bronze_path,
                'silver': silver_path,
                'record_count': len(cleaned_df)
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

def main():
    """Main execution function."""
    # You can set your NASA API key here or use DEMO_KEY
    # api_key = "YOUR_ACTUAL_API_KEY"  # Replace with your key
    api_key = "DEMO_KEY"  # Using demo key for now
    
    ingestor = NASADONKIIngestion(api_key=api_key)
    
    try:
        # Run the pipeline
        results = ingestor.run_ingestion_pipeline(days_back=365)
        
        if results:
            print(f"✅ NASA ingestion successful!")
            print(f"📊 Records processed: {results['record_count']}")
            print(f"📁 Bronze layer: {results['bronze']}")
            print(f"📁 Silver layer: {results['silver']}")
        else:
            print("❌ NASA ingestion failed")
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()





