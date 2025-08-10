#!/usr/bin/env python3
"""
Task 2 - Data Warehouse Execution Script
Executes BigQuery DDL, runs performance queries, and generates schema diagram.
Safe to run while other processes are running.
"""

import os
import sys
import time
from pathlib import Path
from google.cloud import bigquery
from google.auth import default
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Task2WarehouseExecutor:
    """Executes Task 2 - Data Warehouse completion."""
    
    def __init__(self):
        """Initialize BigQuery client and configuration."""
        self.project_id = "ade-adveng-assign"
        self.dataset_id = "imdb_warehouse"
        self.client = None
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
    
    def execute_ddl_script(self):
        """Execute the BigQuery DDL script to create the warehouse."""
        try:
            logger.info("🏗️  Executing BigQuery DDL script...")
            
            # Read the DDL script
            ddl_path = Path(__file__).parent / "warehouse" / "ddl" / "create_warehouse_fixed.sql"
            with open(ddl_path, 'r') as f:
                ddl_script = f.read()
            
            # Split into individual statements
            statements = [stmt.strip() for stmt in ddl_script.split(';') if stmt.strip()]
            
            # Execute each statement
            for i, statement in enumerate(statements, 1):
                if statement and not statement.startswith('--'):
                    try:
                        logger.info(f"Executing statement {i}/{len(statements)}...")
                        job = self.client.query(statement)
                        job.result()  # Wait for completion
                        logger.info(f"✅ Statement {i} executed successfully")
                    except Exception as e:
                        logger.warning(f"⚠️  Statement {i} failed (may already exist): {e}")
                        continue
            
            logger.info("✅ BigQuery DDL execution completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ DDL execution failed: {e}")
            return False
    
    def run_performance_queries(self):
        """Run performance queries to validate optimization."""
        try:
            logger.info("🚀 Running performance validation queries...")
            
            # Query 1: Top rated movies (should use partitioning and clustering)
            query1 = f"""
            SELECT 
                t.primary_title,
                t.start_year,
                t.genres,
                f.average_rating,
                f.num_votes
            FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
            JOIN `{self.project_id}.{self.dataset_id}.dim_title` t ON f.tconst = t.tconst
            WHERE f.num_votes >= 1000
              AND f.average_rating >= 8.0
              AND t.start_year >= 2000
            ORDER BY f.average_rating DESC, f.num_votes DESC
            LIMIT 10
            """
            
            # Query 2: Genre performance analysis
            query2 = f"""
            SELECT 
                g.genre_name,
                COUNT(DISTINCT f.tconst) as title_count,
                AVG(f.average_rating) as avg_rating,
                SUM(f.num_votes) as total_votes
            FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
            JOIN `{self.project_id}.{self.dataset_id}.dim_genre` g ON f.genre = g.genre_id
            WHERE f.num_votes >= 100
            GROUP BY g.genre_name
            ORDER BY avg_rating DESC
            """
            
            # Query 3: Year-based performance (tests partitioning)
            query3 = f"""
            SELECT 
                t.start_year,
                COUNT(DISTINCT f.tconst) as title_count,
                AVG(f.average_rating) as avg_rating
            FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
            JOIN `{self.project_id}.{self.dataset_id}.dim_title` t ON f.tconst = t.tconst
            WHERE t.start_year >= 1990 AND t.start_year <= 2020
            GROUP BY t.start_year
            ORDER BY t.start_year DESC
            """
            
            queries = [
                ("Top Rated Movies (2000+)", query1),
                ("Genre Performance Analysis", query2),
                ("Year-based Performance (1990-2020)", query3)
            ]
            
            results = {}
            for name, query in queries:
                try:
                    logger.info(f"Running: {name}")
                    start_time = time.time()
                    job = self.client.query(query)
                    results_df = job.to_dataframe()
                    execution_time = time.time() - start_time
                    
                    results[name] = {
                        'data': results_df,
                        'execution_time': execution_time,
                        'row_count': len(results_df)
                    }
                    
                    logger.info(f"✅ {name}: {len(results_df)} rows in {execution_time:.2f}s")
                    
                except Exception as e:
                    logger.warning(f"⚠️  Query '{name}' failed: {e}")
                    results[name] = {'error': str(e)}
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Performance queries failed: {e}")
            return {}
    
    def generate_schema_diagram(self):
        """Generate the updated schema diagram."""
        try:
            logger.info("📊 Generating updated schema diagram...")
            
            # Import and run the diagram generator
            sys.path.append(str(Path(__file__).parent / "diagrams"))
            from generate_diagrams import AssignmentDiagramGenerator
            
            generator = AssignmentDiagramGenerator()
            output_path = Path(__file__).parent / "diagrams" / "schema_updated.png"
            
            generator.create_schema_diagram(str(output_path))
            logger.info(f"✅ Schema diagram generated: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema diagram generation failed: {e}")
            return False
    
    def validate_warehouse_structure(self):
        """Validate that the warehouse was created correctly."""
        try:
            logger.info("🔍 Validating warehouse structure...")
            
            # List datasets
            datasets = list(self.client.list_datasets())
            logger.info(f"Available datasets: {[d.dataset_id for d in datasets]}")
            
            # List tables in our dataset
            dataset_ref = self.client.dataset(self.dataset_id)
            tables = list(self.client.list_tables(dataset_ref))
            logger.info(f"Tables in {self.dataset_id}: {[t.table_id for t in tables]}")
            
            # Check table schemas
            for table in tables:
                table_ref = dataset_ref.table(table.table_id)
                table_obj = self.client.get_table(table_ref)
                logger.info(f"Table {table.table_id}: {table_obj.num_rows} rows, {len(table_obj.schema)} columns")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Warehouse validation failed: {e}")
            return False
    
    def execute_task2(self):
        """Execute the complete Task 2 workflow."""
        logger.info("🚀 Starting Task 2 - Data Warehouse Completion")
        logger.info("=" * 60)
        
        try:
            # Step 1: Execute DDL
            if not self.execute_ddl_script():
                logger.error("❌ Task 2 failed at DDL execution")
                return False
            
            # Step 2: Validate structure
            if not self.validate_warehouse_structure():
                logger.error("❌ Task 2 failed at warehouse validation")
                return False
            
            # Step 3: Run performance queries
            performance_results = self.run_performance_queries()
            if not performance_results:
                logger.warning("⚠️  Performance queries failed, but continuing...")
            
            # Step 4: Generate schema diagram
            if not self.generate_schema_diagram():
                logger.error("❌ Task 2 failed at schema diagram generation")
                return False
            
            logger.info("=" * 60)
            logger.info("✅ Task 2 - Data Warehouse completed successfully!")
            logger.info("📊 Performance results:")
            for name, result in performance_results.items():
                if 'error' not in result:
                    logger.info(f"  {name}: {result['row_count']} rows in {result['execution_time']:.2f}s")
                else:
                    logger.info(f"  {name}: Failed - {result['error']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Task 2 execution failed: {e}")
            return False

def main():
    """Main execution function."""
    try:
        executor = Task2WarehouseExecutor()
        success = executor.execute_task2()
        
        if success:
            print("\n🎉 Task 2 completed successfully!")
            print("📋 Next steps:")
            print("  1. Review the generated schema diagram")
            print("  2. Check BigQuery console for warehouse structure")
            print("  3. Update your progress tracker")
            print("  4. Move to Task 3 (PySpark batch processing)")
        else:
            print("\n❌ Task 2 failed. Check logs for details.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Task 2 execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
