#!/usr/bin/env python3
"""
Data Visualization Script - Task 4
Creates visualizations from PySpark processed data for the assignment.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IMDbVisualizer:
    """Creates visualizations from IMDb batch processed data."""
    
    def __init__(self):
        self.gold_dir = "data/gold"
        self.output_dir = "viz/output"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def load_gold_data(self) -> dict:
        """Load processed data from gold layer."""
        dataframes = {}
        
        try:
            for item in os.listdir(self.gold_dir):
                item_path = os.path.join(self.gold_dir, item)
                
                # Check if it's a directory (PySpark output structure)
                if os.path.isdir(item_path):
                    # Extract table name from directory name
                    table_name = item.split('_')[0] + '_' + item.split('_')[1]
                    
                    # Look for Parquet files inside the directory
                    parquet_files = [f for f in os.listdir(item_path) 
                                   if f.endswith('.parquet') and not f.startswith('.')]
                    
                    if parquet_files:
                        # Read the parquet directory
                        df = pd.read_parquet(item_path)
                        dataframes[table_name] = df
                        logger.info(f"Loaded {table_name}: {len(df)} records from {item_path}")
                
                # Also check for direct Parquet files (fallback)
                elif item.endswith('.parquet') and not item.startswith('.'):
                    table_name = item.split('_')[0]
                    df = pd.read_parquet(item_path)
                    dataframes[table_name] = df
                    logger.info(f"Loaded {table_name}: {len(df)} records from {item_path}")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to load gold data: {e}")
            return {}
    
    def create_rating_trends_chart(self, df: pd.DataFrame):
        """Create rating trends over time chart."""
        plt.figure(figsize=(12, 6))
        
        # Group by year and calculate average rating
        yearly_ratings = df.groupby('start_year')['avg_rating'].mean().reset_index()
        
        plt.plot(yearly_ratings['start_year'], yearly_ratings['avg_rating'], 
                marker='o', linewidth=2, markersize=6)
        plt.title('IMDb Average Ratings Over Time', fontsize=16, fontweight='bold')
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Average Rating', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        # Save chart
        output_path = os.path.join(self.output_dir, "rating_trends.png")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Rating trends chart saved: {output_path}")
        return output_path
    
    def create_genre_performance_chart(self, df: pd.DataFrame):
        """Create genre performance comparison chart."""
        # Group by genre and calculate metrics
        genre_stats = df.groupby('genre').agg({
            'title_count': 'sum',
            'avg_rating': 'mean',
            'total_votes': 'sum'
        }).reset_index()
        
        # Limit to top 15 genres for readability
        top_genres = genre_stats.sort_values('title_count', ascending=False).head(15)
        
        # Create subplot with larger height for better label spacing
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 12))
        
        # Chart 1: Title count by genre (top 15)
        genre_counts = top_genres.sort_values('title_count', ascending=True)
        ax1.barh(genre_counts['genre'], genre_counts['title_count'])
        ax1.set_title('Number of Titles by Genre', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Number of Titles')
        
        # Chart 2: Average rating by genre (same top 15)
        genre_ratings = top_genres.sort_values('avg_rating', ascending=True)
        ax2.barh(genre_ratings['genre'], genre_ratings['avg_rating'])
        ax2.set_title('Average Rating by Genre', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Average Rating')
        
        # Adjust layout with more space for labels
        plt.tight_layout()
        plt.subplots_adjust(left=0.15)
        
        # Save chart
        output_path = os.path.join(self.output_dir, "genre_performance.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Genre performance chart saved: {output_path}")
        return output_path
    
    def create_decade_analysis_chart(self, df: pd.DataFrame):
        """Create decade analysis chart."""
        plt.figure(figsize=(12, 8))
        
        # Create decade column if not exists
        if 'decade' not in df.columns:
            df['decade'] = (df['start_year'] // 10) * 10
        
        # Group by decade
        decade_stats = df.groupby('decade').agg({
            'total_titles': 'sum',
            'avg_rating': 'mean'
        }).reset_index()
        
        # Create dual-axis chart
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Bar chart for title count
        bars = ax1.bar(decade_stats['decade'], decade_stats['total_titles'], 
                       alpha=0.7, color='skyblue')
        ax1.set_xlabel('Decade', fontsize=12)
        ax1.set_ylabel('Number of Titles', fontsize=12, color='skyblue')
        ax1.tick_params(axis='y', labelcolor='skyblue')
        
        # Line chart for average rating
        ax2 = ax1.twinx()
        line = ax2.plot(decade_stats['decade'], decade_stats['avg_rating'], 
                       color='red', marker='o', linewidth=2)
        ax2.set_ylabel('Average Rating', fontsize=12, color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        plt.title('IMDb Titles and Ratings by Decade', fontsize=16, fontweight='bold')
        
        # Save chart
        output_path = os.path.join(self.output_dir, "decade_analysis.png")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Decade analysis chart saved: {output_path}")
        return output_path
    
    def create_summary_dashboard(self, dataframes: dict):
        """Create a summary dashboard with key metrics."""
        plt.figure(figsize=(16, 12))
        
        # Create subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Summary statistics
        total_titles = sum(len(df) for df in dataframes.values())
        avg_rating = 0
        total_votes = 0
        
        if 'title_ratings' in dataframes:
            df = dataframes['title_ratings']
            if 'avg_rating' in df.columns:
                avg_rating = df['avg_rating'].mean()
            if 'total_votes' in df.columns:
                total_votes = df['total_votes'].sum()
        
        # Metric 1: Total titles
        ax1.text(0.5, 0.5, f'{total_titles:,}', fontsize=24, ha='center', va='center')
        ax1.set_title('Total Titles Processed', fontsize=14, fontweight='bold')
        ax1.axis('off')
        
        # Metric 2: Average rating
        ax2.text(0.5, 0.5, f'{avg_rating:.2f}', fontsize=24, ha='center', va='center')
        ax2.set_title('Overall Average Rating', fontsize=14, fontweight='bold')
        ax2.axis('off')
        
        # Metric 3: Total votes
        ax3.text(0.5, 0.5, f'{total_votes:,}', fontsize=24, ha='center', va='center')
        ax3.set_title('Total Votes', fontsize=14, fontweight='bold')
        ax3.axis('off')
        
        # Metric 4: Data sources
        ax4.text(0.5, 0.5, f'{len(dataframes)}', fontsize=24, ha='center', va='center')
        ax4.set_title('Data Sources', fontsize=14, fontweight='bold')
        ax4.axis('off')
        
        plt.suptitle('IMDb Data Analysis Dashboard', fontsize=20, fontweight='bold')
        
        # Save dashboard
        output_path = os.path.join(self.output_dir, "summary_dashboard.png")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Summary dashboard saved: {output_path}")
        return output_path
    
    def run_visualization_pipeline(self):
        """Run the complete visualization pipeline."""
        logger.info("=== Starting Data Visualization Pipeline ===")
        
        try:
            # Load data
            dataframes = self.load_gold_data()
            
            if not dataframes:
                logger.error("No data loaded from gold layer")
                return {}
            
            results = {}
            
            # Create visualizations
            if 'title_ratings' in dataframes:
                results['rating_trends'] = self.create_rating_trends_chart(
                    dataframes['title_ratings']
                )
            
            if 'genre_analysis' in dataframes:
                results['genre_performance'] = self.create_genre_performance_chart(
                    dataframes['genre_analysis']
                )
            
            if 'decade_trends' in dataframes:
                results['decade_analysis'] = self.create_decade_analysis_chart(
                    dataframes['decade_trends']
                )
            
            # Create summary dashboard
            results['summary_dashboard'] = self.create_summary_dashboard(dataframes)
            
            logger.info("=== Data Visualization Pipeline Complete ===")
            return results
            
        except Exception as e:
            logger.error(f"Visualization pipeline failed: {e}")
            raise

def main():
    """Main execution function."""
    try:
        visualizer = IMDbVisualizer()
        results = visualizer.run_visualization_pipeline()
        
        if results:
            print("✅ Data visualization successful!")
            print("📊 Generated charts:")
            for chart_type, path in results.items():
                print(f"   - {chart_type}: {path}")
        else:
            print("❌ Data visualization failed")
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()



