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
        self.project_id = "your-gcp-project-id"  # Replace with your actual project ID
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
        
        result1 = self.run_performance_query("Partition Test - 2020", query1)
        
        # Test 2: Query range of years
        query2 = f"""
        SELECT start_year, COUNT(*) as title_count, AVG(runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE start_year BETWEEN 2015 AND 2020
        GROUP BY start_year
        ORDER BY start_year
        """
        
        result2 = self.run_performance_query("Partition Test - Year Range", query2)
        
        # Test 3: Query without partition filter (should be slower)
        query3 = f"""
        SELECT COUNT(*) as total_titles, AVG(runtime_minutes) as overall_avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        """
        
        result3 = self.run_performance_query("Partition Test - No Filter", query3)
        
        return [result1, result2, result3]
    
    def test_clustering_performance(self):
        """Test clustering performance with genre-based queries."""
        logger.info("🎭 Testing clustering performance...")
        
        # Test 1: Query specific genre (clustered field)
        query1 = f"""
        SELECT COUNT(*) as title_count, AVG(runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE 'Action' IN UNNEST(genres)
        """
        
        result1 = self.run_performance_query("Clustering Test - Action Genre", query1)
        
        # Test 2: Query multiple genres
        query2 = f"""
        SELECT COUNT(*) as title_count, AVG(runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE 'Drama' IN UNNEST(genres) OR 'Comedy' IN UNNEST(genres)
        """
        
        result2 = self.run_performance_query("Clustering Test - Multiple Genres", query2)
        
        # Test 3: Query without clustering benefit
        query3 = f"""
        SELECT COUNT(*) as title_count, AVG(runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title`
        WHERE runtime_minutes > 120
        """
        
        result3 = self.run_performance_query("Clustering Test - No Clustering Benefit", query3)
        
        return [result1, result2, result3]
    
    def test_view_performance(self):
        """Test materialized view performance."""
        logger.info("👁️  Testing view performance...")
        
        # Test 1: Query the materialized view
        query1 = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.vw_title_ratings_summary`
        WHERE avg_rating >= 8.0
        ORDER BY avg_rating DESC
        LIMIT 100
        """
        
        result1 = self.run_performance_query("View Test - High Ratings", query1)
        
        # Test 2: Compare with base table query
        query2 = f"""
        SELECT 
            t.primary_title,
            t.start_year,
            t.genres,
            r.averageRating,
            r.numVotes
        FROM `{self.project_id}.{self.dataset_id}.dim_title` t
        JOIN `{self.project_id}.{self.dataset_id}.fact_title_rating` r
        ON t.tconst = r.tconst
        WHERE r.averageRating >= 8.0
        ORDER BY r.averageRating DESC
        LIMIT 100
        """
        
        result2 = self.run_performance_query("View Test - Base Table Comparison", query2)
        
        return [result1, result2]
    
    def test_complex_analytics(self):
        """Test complex analytical queries."""
        logger.info("🧮 Testing complex analytics...")
        
        # Test 1: Genre performance analysis
        query1 = f"""
        WITH genre_stats AS (
            SELECT 
                genre,
                COUNT(*) as title_count,
                AVG(r.averageRating) as avg_rating,
                SUM(r.numVotes) as total_votes
            FROM `{self.project_id}.{self.dataset_id}.dim_title` t,
            UNNEST(t.genres) as genre
            JOIN `{self.project_id}.{self.dataset_id}.fact_title_rating` r
            ON t.tconst = r.tconst
            WHERE r.numVotes >= 1000
            GROUP BY genre
        )
        SELECT 
            genre,
            title_count,
            ROUND(avg_rating, 2) as avg_rating,
            total_votes,
            ROUND(avg_rating * LOG10(total_votes), 2) as weighted_score
        FROM genre_stats
        ORDER BY weighted_score DESC
        LIMIT 20
        """
        
        result1 = self.run_performance_query("Complex Analytics - Genre Performance", query1)
        
        # Test 2: Decade analysis
        query2 = f"""
        SELECT 
            FLOOR(start_year / 10) * 10 as decade,
            COUNT(*) as title_count,
            AVG(r.averageRating) as avg_rating,
            AVG(t.runtime_minutes) as avg_runtime
        FROM `{self.project_id}.{self.dataset_id}.dim_title` t
        JOIN `{self.project_id}.{self.dataset_id}.fact_title_rating` r
        ON t.tconst = r.tconst
        WHERE start_year BETWEEN 1900 AND 2020
        GROUP BY decade
        ORDER BY decade
        """
        
        result2 = self.run_performance_query("Complex Analytics - Decade Analysis", query2)
        
        return [result1, result2]
    
    def run_all_performance_tests(self):
        """Run all performance tests and return results."""
        logger.info("🚀 Starting comprehensive warehouse performance testing...")
        
        all_results = []
        
        try:
            # Test partitioning
            partitioning_results = self.test_partitioning_performance()
            all_results.extend(partitioning_results)
            
            # Test clustering
            clustering_results = self.test_clustering_performance()
            all_results.extend(clustering_results)
            
            # Test views
            view_results = self.test_view_performance()
            all_results.extend(view_results)
            
            # Test complex analytics
            analytics_results = self.test_complex_analytics()
            all_results.extend(analytics_results)
            
            # Summary
            successful_tests = sum(1 for r in all_results if r['success'])
            total_tests = len(all_results)
            
            logger.info(f"🎉 Performance testing completed: {successful_tests}/{total_tests} tests passed")
            
            return all_results
            
        except Exception as e:
            logger.error(f"❌ Performance testing failed: {e}")
            raise

def main():
    """Main execution function."""
    try:
        tester = WarehousePerformanceTester()
        results = tester.run_all_performance_tests()
        
        print("\n📊 Warehouse Performance Test Results:")
        print("=" * 50)
        
        for result in results:
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            print(f"{status} {result['query_name']}")
            if result['success']:
                print(f"   ⏱️  Execution time: {result['execution_time']:.2f}s")
                print(f"   💾 Bytes processed: {result['bytes_processed']:,}")
            else:
                print(f"   ❌ Error: {result.get('error', 'Unknown error')}")
            print()
        
        successful_tests = sum(1 for r in results if r['success'])
        total_tests = len(results)
        
        print(f"🎯 Overall Results: {successful_tests}/{total_tests} tests passed")
        
        if successful_tests == total_tests:
            print("🎉 All performance tests passed! Your warehouse is optimized.")
        else:
            print("⚠️  Some tests failed. Check the logs for details.")
            
    except Exception as e:
        print(f"❌ Performance testing failed: {e}")
        raise

if __name__ == "__main__":
    main()
