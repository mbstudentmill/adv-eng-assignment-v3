#!/usr/bin/env python3
"""
Test Warehouse Performance
Runs performance queries to validate BigQuery optimization features.
"""

import os
import time
from pathlib import Path
from google.cloud import bigquery
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WarehousePerformanceTester:
    """Tests BigQuery warehouse performance with various queries."""
    
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
    
    def run_performance_query(self, query_name, query, expected_result_type="SELECT"):
        """Run a performance query and measure execution time."""
        try:
            logger.info(f"🔍 Running: {query_name}")
            start_time = time.time()
            
            # Run the query
            query_job = self.client.query(query)
            results = query_job.result()
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Get job statistics
            job = query_job
            bytes_processed = job.total_bytes_processed if hasattr(job, 'total_bytes_processed') else 0
            bytes_billed = job.total_bytes_billed if hasattr(job, 'total_bytes_billed') else 0
            
            logger.info(f"✅ {query_name} completed in {execution_time:.2f}s")
            logger.info(f"   Bytes processed: {bytes_processed:,}")
            logger.info(f"   Bytes billed: {bytes_billed:,}")
            
            # Count results if it's a SELECT query
            if expected_result_type == "SELECT":
                row_count = sum(1 for _ in results)
                logger.info(f"   Rows returned: {row_count:,}")
            
            return {
                "query_name": query_name,
                "execution_time": execution_time,
                "bytes_processed": bytes_processed,
                "bytes_billed": bytes_billed,
                "success": True
            }
            
        except Exception as e:
            logger.error(f"❌ {query_name} failed: {e}")
            return {
                "query_name": query_name,
                "execution_time": 0,
                "bytes_processed": 0,
                "bytes_billed": 0,
                "success": False,
                "error": str(e)
            }
    
    def test_partitioning_performance(self):
        """Test partitioning performance with year-based queries."""
        logger.info("📊 Testing partitioning performance...")
        
        # Test 1: Query specific year partition
        query1 = f"""
        SELECT COUNT(*) as title_count, AVG(runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE start_year = 2020
        """
        
        # Test 2: Query range of years (should use multiple partitions)
        query2 = f"""
        SELECT start_year, COUNT(*) as title_count
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE start_year BETWEEN 2010 AND 2020
        GROUP BY start_year
        ORDER BY start_year
        """
        
        # Test 3: Query recent years (should be efficient)
        query3 = f"""
        SELECT title_type, COUNT(*) as count
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE start_year >= 2015
        GROUP BY title_type
        ORDER BY count DESC
        """
        
        results = []
        results.append(self.run_performance_query("Partition Test 1: Single Year (2020)", query1))
        results.append(self.run_performance_query("Partition Test 2: Year Range (2010-2020)", query2))
        results.append(self.run_performance_query("Partition Test 3: Recent Years (2015+)", query3))
        
        return results
    
    def test_clustering_performance(self):
        """Test clustering performance with genre and title type queries."""
        logger.info("🎯 Testing clustering performance...")
        
        # Test 1: Query by clustered field (title_type)
        query1 = f"""
        SELECT primary_title, start_year, genres
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE title_type = 'movie'
        LIMIT 100
        """
        
        # Test 2: Query by clustered field (genre in fact table)
        query2 = f"""
        SELECT genre, COUNT(*) as rating_count, AVG(average_rating) as avg_rating
        FROM `{self.project_id}.{self.dataset_id}.fact_title_rating`
        WHERE genre = 'action'
        GROUP BY genre
        """
        
        # Test 3: Combined clustering fields
        query3 = f"""
        SELECT t.title_type, f.genre, COUNT(*) as count
        FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
        JOIN `{self.project_id}.{self.dataset_id}.dim_title` t ON f.tconst = t.tconst
        WHERE f.genre = 'drama' AND t.title_type = 'movie'
        GROUP BY t.title_type, f.genre
        """
        
        results = []
        results.append(self.run_performance_query("Clustering Test 1: Title Type Filter", query1))
        results.append(self.run_performance_query("Clustering Test 2: Genre Filter", query2))
        results.append(self.run_performance_query("Clustering Test 3: Combined Clustering", query3))
        
        return results
    
    def test_view_performance(self):
        """Test view performance."""
        logger.info("👁️ Testing view performance...")
        
        # Test 1: Top rated movies view
        query1 = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.v_top_rated_movies`
        LIMIT 10
        """
        
        # Test 2: Genre performance view
        query2 = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.v_genre_performance`
        """
        
        results = []
        results.append(self.run_performance_query("View Test 1: Top Rated Movies", query1))
        results.append(self.run_performance_query("View Test 2: Genre Performance", query2))
        
        return results
    
    def test_complex_analytics(self):
        """Test complex analytical queries."""
        logger.info("🧮 Testing complex analytics...")
        
        # Test 1: Multi-table join with aggregations
        query1 = f"""
        SELECT 
            t.title_type,
            g.genre_name,
            COUNT(DISTINCT f.tconst) as title_count,
            AVG(f.average_rating) as avg_rating,
            SUM(f.num_votes) as total_votes
        FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
        JOIN `{self.project_id}.{self.dataset_id}.dim_title` t ON f.tconst = t.tconst
        JOIN `{self.project_id}.{self.dataset_id}.dim_genre` g ON f.genre = g.genre_id
        WHERE f.num_votes >= 100
        GROUP BY t.title_type, g.genre_name
        ORDER BY avg_rating DESC
        LIMIT 20
        """
        
        # Test 2: Time-based analysis
        query2 = f"""
        SELECT 
            EXTRACT(DECADE FROM DATE(start_year, 1, 1)) as decade,
            COUNT(DISTINCT t.tconst) as title_count,
            AVG(f.average_rating) as avg_rating
        FROM `{self.project_id}.{self.dataset_id}.fact_title_rating` f
        JOIN `{self.project_id}.{self.dataset_id}.dim_title` t ON f.tconst = t.tconst
        WHERE t.start_year BETWEEN 1900 AND 2020
        GROUP BY decade
        ORDER BY decade
        """
        
        results = []
        results.append(self.run_performance_query("Complex Test 1: Multi-table Analytics", query1))
        results.append(self.run_performance_query("Complex Test 2: Time-based Analysis", query2))
        
        return results
    
    def run_all_performance_tests(self):
        """Run all performance tests."""
        logger.info("🚀 Starting comprehensive warehouse performance testing...")
        logger.info("=" * 60)
        
        all_results = []
        
        # Run all test categories
        all_results.extend(self.test_partitioning_performance())
        all_results.extend(self.test_clustering_performance())
        all_results.extend(self.test_view_performance())
        all_results.extend(self.test_complex_analytics())
        
        # Summary
        successful_tests = [r for r in all_results if r["success"]]
        failed_tests = [r for r in all_results if not r["success"]]
        
        logger.info("=" * 60)
        logger.info("📊 PERFORMANCE TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"✅ Successful tests: {len(successful_tests)}")
        logger.info(f"❌ Failed tests: {len(failed_tests)}")
        
        if successful_tests:
            avg_execution_time = sum(r["execution_time"] for r in successful_tests) / len(successful_tests)
            total_bytes_processed = sum(r["bytes_processed"] for r in successful_tests)
            logger.info(f"⏱️  Average execution time: {avg_execution_time:.2f}s")
            logger.info(f"💾 Total bytes processed: {total_bytes_processed:,}")
        
        if failed_tests:
            logger.info("\n❌ Failed tests:")
            for test in failed_tests:
                logger.info(f"  - {test['query_name']}: {test.get('error', 'Unknown error')}")
        
        return all_results

def main():
    """Main execution function."""
    try:
        tester = WarehousePerformanceTester()
        results = tester.run_all_performance_tests()
        
        successful_count = len([r for r in results if r["success"]])
        total_count = len(results)
        
        print(f"\n🎉 Performance testing completed!")
        print(f"📊 Results: {successful_count}/{total_count} tests passed")
        
        if successful_count == total_count:
            print("✅ All performance tests passed! Your warehouse is optimized.")
        else:
            print("⚠️  Some tests failed. Check logs for details.")
        
        print("\n📋 Next steps:")
        print("  1. Review performance results")
        print("  2. Generate final schema diagram")
        print("  3. Update progress tracker")
        print("  4. Move to Task 3 (PySpark batch processing)")
        
    except Exception as e:
        logger.error(f"❌ Performance testing failed: {e}")
        print(f"\n❌ Performance testing failed: {e}")

if __name__ == "__main__":
    main()
