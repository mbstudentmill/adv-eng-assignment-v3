#!/usr/bin/env python3
"""
PySpark Batch Processing Script - Task 3
Processes IMDb data warehouse data using PySpark for batch analytics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IMDbBatchProcessor:
    """PySpark batch processor for IMDb data warehouse."""
    
    def __init__(self, app_name: str = "IMDb-Batch-Processing"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf=") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf=") \
            .getOrCreate()
        
        logger.info("PySpark session initialized")
    
    def load_silver_data(self, silver_dir: str = "data/silver") -> dict:
        """Load data from silver layer."""
        dataframes = {}
        
        try:
            # Load IMDb silver data - files are named directly without 'imdb' prefix
            for file in os.listdir(silver_dir):
                if file.endswith('.parquet') and not file.startswith('nasa'):
                    table_name = file.replace('.parquet', '')
                    file_path = os.path.join(silver_dir, file)
                    
                    df = self.spark.read.parquet(file_path)
                    dataframes[table_name] = df
                    logger.info(f"Loaded {table_name}: {df.count()} records")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to load silver data: {e}")
            raise
    
    def process_title_ratings(self, title_df, rating_df):
        """Process title ratings aggregations."""
        logger.info("Processing title ratings...")
        
        # Join title basics with ratings
        joined_df = title_df.join(rating_df, "tconst")
        
        # Aggregate by startYear and genres
        aggregated = joined_df.groupBy("startYear", "genres") \
            .agg(
                count("*").alias("total_titles"),
                avg("averageRating").alias("avg_rating"),
                sum("numVotes").alias("total_votes"),
                max("averageRating").alias("max_rating"),
                min("averageRating").alias("min_rating")
            )
        
        return aggregated
    
    def process_genre_analysis(self, title_df, rating_df):
        """Process genre analysis aggregations."""
        logger.info("Processing genre analysis...")
        
        # Join title basics with ratings
        joined_df = title_df.join(rating_df, "tconst")
        
        # Aggregate by genres
        aggregated = joined_df.groupBy("genres") \
            .agg(
                count("*").alias("total_titles"),
                avg("averageRating").alias("avg_rating"),
                sum("numVotes").alias("total_votes"),
                avg("startYear").alias("avg_year")
            )
        
        return aggregated
    
    def process_decade_trends(self, title_df, rating_df):
        """Process decade trends analysis."""
        logger.info("Processing decade trends...")
        
        # Join title basics with ratings
        joined_df = title_df.join(rating_df, "tconst")
        
        # Create decade column from startYear
        decade_df = joined_df.withColumn("decade", 
            (col("startYear") / 10).cast("int") * 10)
        
        # Aggregate by decade
        aggregated = decade_df.groupBy("decade") \
            .agg(
                count("*").alias("total_titles"),
                avg("averageRating").alias("avg_rating"),
                sum("numVotes").alias("total_votes")
            ) \
            .filter(col("decade").isNotNull())
        
        return aggregated
    
    def save_gold_data(self, df, table_name: str, gold_dir: str = "data/gold"):
        """Save processed data to gold layer."""
        os.makedirs(gold_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(gold_dir, f"{table_name}_{timestamp}")
        
        try:
            df.write.mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(output_path)
            
            logger.info(f"Saved {table_name} to gold layer: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save {table_name}: {e}")
            raise
    
    def run_batch_pipeline(self):
        """Run the complete batch processing pipeline."""
        logger.info("=== Starting PySpark Batch Processing Pipeline ===")
        
        try:
            # Load data
            dataframes = self.load_silver_data()
            
            if not dataframes:
                logger.error("No data loaded from silver layer")
                return {}
            
            results = {}
            
            # Process different aggregations
            if 'title.basics' in dataframes and 'title.ratings' in dataframes:
                # Title ratings analysis
                title_ratings = self.process_title_ratings(
                    dataframes['title.basics'], 
                    dataframes['title.ratings']
                )
                results['title_ratings'] = self.save_gold_data(
                    title_ratings, "title_ratings_agg"
                )
                
                # Genre analysis
                genre_analysis = self.process_genre_analysis(
                    dataframes['title.basics'], 
                    dataframes['title.ratings']
                )
                results['genre_analysis'] = self.save_gold_data(
                    genre_analysis, "genre_analysis"
                )
                
                # Decade trends
                decade_trends = self.process_decade_trends(
                    dataframes['title.basics'], 
                    dataframes['title.ratings']
                )
                results['decade_trends'] = self.save_gold_data(
                    decade_trends, "decade_trends"
                )
            
            logger.info("=== PySpark Batch Processing Pipeline Complete ===")
            return results
            
        except Exception as e:
            logger.error(f"Batch pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Main execution function."""
    try:
        processor = IMDbBatchProcessor()
        results = processor.run_batch_pipeline()
        
        if results:
            print("✅ PySpark batch processing successful!")
            for table, path in results.items():
                print(f"📊 {table}: {path}")
        else:
            print("❌ PySpark batch processing failed")
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()





