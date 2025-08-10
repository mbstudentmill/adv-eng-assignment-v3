#!/usr/bin/env python3
"""
Create BigQuery Warehouse Tables Directly
Creates the IMDb data warehouse tables using BigQuery client.
"""

import os
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WarehouseTableCreator:
    """Creates BigQuery warehouse tables directly."""
    
    def __init__(self):
        """Initialize BigQuery client."""
        self.project_id = "ade-adveng-assign"
        self.dataset_id = "imdb_warehouse"
        self.setup_bigquery_client()
        
    def setup_bigquery_client(self):
        """Setup BigQuery client with authentication."""
        try:
            # Set credentials path
            credentials_path = Path(__file__).parent / "gcp-credentials.json"
            if credentials_path.exists():
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_path)
                logger.info(f"Using credentials from: {credentials_path}")
            else:
                logger.warning("gcp-credentials.json not found, using default authentication")
            
            # Initialize BigQuery client
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"✅ BigQuery client initialized for project: {self.project_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup BigQuery client: {e}")
            raise
    
    def create_dim_title_table(self):
        """Create the dim_title dimension table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_title"
            
            schema = [
                SchemaField("tconst", "STRING", mode="REQUIRED"),
                SchemaField("title_type", "STRING"),
                SchemaField("primary_title", "STRING"),
                SchemaField("original_title", "STRING"),
                SchemaField("is_adult", "BOOLEAN"),
                SchemaField("start_year", "INTEGER"),
                SchemaField("end_year", "INTEGER"),
                SchemaField("runtime_minutes", "INTEGER"),
                SchemaField("genres", "STRING", mode="REPEATED"),
                SchemaField("created_date", "TIMESTAMP"),
                SchemaField("updated_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.partitioning = bigquery.RangePartitioning(
                field="start_year",
                range_=bigquery.PartitionRange(start=1888, end=2030, interval=1)
            )
            table.clustering_fields = ["title_type", "genres"]
            
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dim_title table: {e}")
            return False
    
    def create_dim_person_table(self):
        """Create the dim_person dimension table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_person"
            
            schema = [
                SchemaField("nconst", "STRING", mode="REQUIRED"),
                SchemaField("primary_name", "STRING"),
                SchemaField("birth_year", "INTEGER"),
                SchemaField("death_year", "INTEGER"),
                SchemaField("primary_profession", "STRING", mode="REPEATED"),
                SchemaField("known_for_titles", "STRING", mode="REPEATED"),
                SchemaField("created_date", "TIMESTAMP"),
                SchemaField("updated_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["primary_profession"]
            
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dim_person table: {e}")
            return False
    
    def create_dim_genre_table(self):
        """Create the dim_genre dimension table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_genre"
            
            schema = [
                SchemaField("genre_id", "STRING", mode="REQUIRED"),
                SchemaField("genre_name", "STRING", mode="REQUIRED"),
                SchemaField("genre_description", "STRING"),
                SchemaField("created_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dim_genre table: {e}")
            return False
    
    def create_dim_region_table(self):
        """Create the dim_region dimension table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_region"
            
            schema = [
                SchemaField("region_id", "STRING", mode="REQUIRED"),
                SchemaField("region_name", "STRING", mode="REQUIRED"),
                SchemaField("language", "STRING"),
                SchemaField("country", "STRING"),
                SchemaField("created_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dim_region table: {e}")
            return False
    
    def create_dim_date_table(self):
        """Create the dim_date dimension table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_date"
            
            schema = [
                SchemaField("date_id", "STRING", mode="REQUIRED"),
                SchemaField("full_date", "DATE", mode="REQUIRED"),
                SchemaField("year", "INTEGER", mode="REQUIRED"),
                SchemaField("month", "INTEGER", mode="REQUIRED"),
                SchemaField("day", "INTEGER", mode="REQUIRED"),
                SchemaField("quarter", "INTEGER", mode="REQUIRED"),
                SchemaField("day_of_week", "INTEGER", mode="REQUIRED"),
                SchemaField("day_name", "STRING", mode="REQUIRED"),
                SchemaField("month_name", "STRING", mode="REQUIRED"),
                SchemaField("is_weekend", "BOOLEAN", mode="REQUIRED"),
                SchemaField("created_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.partitioning = bigquery.RangePartitioning(
                field="year",
                range_=bigquery.PartitionRange(start=1888, end=2030, interval=1)
            )
            
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dim_date table: {e}")
            return False
    
    def create_bridge_title_genre_table(self):
        """Create the bridge_title_genre table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.bridge_title_genre"
            
            schema = [
                SchemaField("tconst", "STRING", mode="REQUIRED"),
                SchemaField("genre_id", "STRING", mode="REQUIRED"),
                SchemaField("created_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.clustering_fields = ["tconst", "genre_id"]
            
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create bridge_title_genre table: {e}")
            return False
    
    def create_fact_title_rating_table(self):
        """Create the fact_title_rating fact table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.fact_title_rating"
            
            schema = [
                SchemaField("fact_id", "STRING", mode="REQUIRED"),
                SchemaField("tconst", "STRING", mode="REQUIRED"),
                SchemaField("nconst", "STRING"),
                SchemaField("average_rating", "FLOAT"),
                SchemaField("num_votes", "INTEGER"),
                SchemaField("start_year", "INTEGER"),
                SchemaField("genre", "STRING"),
                SchemaField("region", "STRING"),
                SchemaField("rating_date", "DATE"),
                SchemaField("created_date", "TIMESTAMP"),
                SchemaField("updated_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table.partitioning = bigquery.RangePartitioning(
                field="start_year",
                range_=bigquery.PartitionRange(start=1888, end=2030, interval=1)
            )
            table.clustering_fields = ["tconst", "genre", "average_rating"]
            
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create fact_title_rating table: {e}")
            return False
    
    def create_etl_audit_log_table(self):
        """Create the ETL audit log table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.etl_audit_log"
            
            schema = [
                SchemaField("log_id", "STRING", mode="REQUIRED"),
                SchemaField("table_name", "STRING", mode="REQUIRED"),
                SchemaField("operation", "STRING", mode="REQUIRED"),
                SchemaField("records_processed", "INTEGER"),
                SchemaField("start_time", "TIMESTAMP"),
                SchemaField("end_time", "TIMESTAMP"),
                SchemaField("status", "STRING"),
                SchemaField("error_message", "STRING"),
                SchemaField("created_date", "TIMESTAMP")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Created table: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create etl_audit_log table: {e}")
            return False
    
    def insert_sample_genre_data(self):
        """Insert sample data into the dim_genre table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.dim_genre"
            
            sample_data = [
                {"genre_id": "action", "genre_name": "Action", "genre_description": "Action films and television shows"},
                {"genre_id": "adventure", "genre_name": "Adventure", "genre_description": "Adventure and exploration content"},
                {"genre_id": "comedy", "genre_name": "Comedy", "genre_description": "Comedic and humorous content"},
                {"genre_id": "drama", "genre_name": "Drama", "genre_description": "Dramatic and serious content"},
                {"genre_id": "horror", "genre_name": "Horror", "genre_description": "Horror and frightening content"},
                {"genre_id": "romance", "genre_name": "Romance", "genre_description": "Romantic and love stories"},
                {"genre_id": "sci_fi", "genre_name": "Science Fiction", "genre_description": "Science fiction and futuristic content"},
                {"genre_id": "thriller", "genre_name": "Thriller", "genre_description": "Suspenseful and thrilling content"}
            ]
            
            errors = self.client.insert_rows_json(table_id, sample_data)
            if errors:
                logger.warning(f"⚠️  Some genre data insert errors: {errors}")
            else:
                logger.info(f"✅ Inserted {len(sample_data)} genre records")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert sample genre data: {e}")
            return False
    
    def create_all_tables(self):
        """Create all warehouse tables."""
        logger.info("🏗️  Creating BigQuery warehouse tables...")
        
        tables_created = []
        
        # Create dimension tables
        if self.create_dim_title_table():
            tables_created.append("dim_title")
        if self.create_dim_person_table():
            tables_created.append("dim_person")
        if self.create_dim_genre_table():
            tables_created.append("dim_genre")
        if self.create_dim_region_table():
            tables_created.append("dim_region")
        if self.create_dim_date_table():
            tables_created.append("dim_date")
        
        # Create bridge table
        if self.create_bridge_title_genre_table():
            tables_created.append("bridge_title_genre")
        
        # Create fact table
        if self.create_fact_title_rating_table():
            tables_created.append("fact_title_rating")
        
        # Create audit table
        if self.create_etl_audit_log_table():
            tables_created.append("etl_audit_log")
        
        # Insert sample data
        self.insert_sample_genre_data()
        
        logger.info(f"✅ Created {len(tables_created)} tables: {', '.join(tables_created)}")
        return tables_created

def main():
    """Main execution function."""
    try:
        creator = WarehouseTableCreator()
        tables = creator.create_all_tables()
        
        if tables:
            print(f"\n🎉 Successfully created {len(tables)} BigQuery tables!")
            print("📋 Tables created:")
            for table in tables:
                print(f"  - {table}")
            print("\n📊 Next steps:")
            print("  1. Run performance queries to validate optimization")
            print("  2. Generate schema diagram")
            print("  3. Update progress tracker")
            print("  4. Move to Task 3 (PySpark batch processing)")
        else:
            print("\n❌ Failed to create tables. Check logs for details.")
            
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        print(f"\n❌ Table creation failed: {e}")

if __name__ == "__main__":
    main()
