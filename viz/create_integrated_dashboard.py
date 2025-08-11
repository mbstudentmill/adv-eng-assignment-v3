#!/usr/bin/env python3
"""
Integrated Dashboard Creator - Task 4
Creates a comprehensive dashboard combining all visualizations for the assignment.
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

class IntegratedDashboardCreator:
    """Creates an integrated dashboard combining all visualizations."""
    
    def __init__(self):
        self.gold_dir = "data/gold"
        self.output_dir = "viz/output"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set style for professional appearance
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (16, 12)
        plt.rcParams['font.size'] = 10
        
    def load_gold_data(self) -> dict:
        """Load processed data from gold layer."""
        dataframes = {}
        
        try:
            for item in os.listdir(self.gold_dir):
                item_path = os.path.join(self.gold_dir, item)
                
                # Check if it's a directory (our gold layer structure)
                if os.path.isdir(item_path):
                    # Extract table name from directory name
                    table_name = item.split('_')[0] + '_' + item.split('_')[1]
                    
                    # Look for Parquet files inside the directory
                    parquet_files = [f for f in os.listdir(item_path) if f.endswith('.parquet') and not f.startswith('.')]
                    
                    if parquet_files:
                        # Read the first Parquet file
                        parquet_path = os.path.join(item_path, parquet_files[0])
                        df = pd.read_parquet(parquet_path)
                        dataframes[table_name] = df
                        logger.info(f"Loaded {table_name}: {len(df)} records from {parquet_path}")
                
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
    
    def create_integrated_dashboard(self, dataframes: dict):
        """Create a comprehensive integrated dashboard."""
        logger.info("Creating integrated dashboard...")
        
        # Create the main figure with subplots
        fig = plt.figure(figsize=(20, 16))
        
        # Create a grid layout: 3 rows, 2 columns
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # Row 1: Title and summary metrics
        title_ax = fig.add_subplot(gs[0, :])
        title_ax.text(0.5, 0.5, 'IMDb Data Analysis Dashboard\nAdvanced Data Engineering Assignment', 
                     fontsize=24, fontweight='bold', ha='center', va='center')
        title_ax.set_xlim(0, 1)
        title_ax.set_ylim(0, 1)
        title_ax.axis('off')
        
        # Row 2: Rating trends and Genre performance
        # Rating trends (left)
        if 'title_ratings' in dataframes:
            ax1 = fig.add_subplot(gs[1, 0])
            df = dataframes['title_ratings']
            yearly_ratings = df.groupby('startYear')['avg_rating'].mean().reset_index()
            
            ax1.plot(yearly_ratings['startYear'], yearly_ratings['avg_rating'], 
                    marker='o', linewidth=2, markersize=6, color='#2E86AB')
            ax1.set_title('IMDb Average Ratings Over Time', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Year', fontsize=12)
            ax1.set_ylabel('Average Rating', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)
        
        # Genre performance (right)
        if 'genre_analysis' in dataframes:
            ax2 = fig.add_subplot(gs[1, 1])
            df = dataframes['genre_analysis']
            
            # Get top 10 genres by total titles
            top_genres = df.nlargest(10, 'total_titles')
            
            bars = ax2.barh(range(len(top_genres)), top_genres['total_titles'], 
                           color='#A23B72', alpha=0.8)
            ax2.set_yticks(range(len(top_genres)))
            ax2.set_yticklabels(top_genres['genres'], fontsize=9)
            ax2.set_title('Top 10 Genres by Number of Titles', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Number of Titles', fontsize=12)
        
        # Row 3: Decade analysis and Summary metrics
        # Decade analysis (left)
        if 'decade_trends' in dataframes:
            ax3 = fig.add_subplot(gs[2, 0])
            df = dataframes['decade_trends']
            
            # Create dual-axis chart
            bars = ax3.bar(df['decade'], df['total_titles'], 
                          alpha=0.7, color='#F18F01')
            ax3.set_xlabel('Decade', fontsize=12)
            ax3.set_ylabel('Number of Titles', fontsize=12, color='#F18F01')
            ax3.tick_params(axis='y', labelcolor='#F18F01')
            
            # Add rating line on secondary axis
            ax3_twin = ax3.twinx()
            line = ax3_twin.plot(df['decade'], df['avg_rating'], 
                               color='#C73E1D', marker='o', linewidth=2)
            ax3_twin.set_ylabel('Average Rating', fontsize=12, color='#C73E1D')
            ax3_twin.tick_params(axis='y', labelcolor='#C73E1D')
            
            ax3.set_title('IMDb Titles and Ratings by Decade', fontsize=14, fontweight='bold')
        
        # Summary metrics (right)
        ax4 = fig.add_subplot(gs[2, 1])
        ax4.axis('off')
        
        # Calculate summary statistics
        total_titles = sum(len(df) for df in dataframes.values())
        avg_rating = 0
        total_votes = 0
        
        if 'title_ratings' in dataframes:
            df = dataframes['title_ratings']
            if 'avg_rating' in df.columns:
                avg_rating = df['avg_rating'].mean()
            if 'total_votes' in df.columns:
                total_votes = df['total_votes'].sum()
        
        # Create summary text
        summary_text = f"""
        📊 DATASET SUMMARY
        
        📈 Total Records: {total_titles:,}
        ⭐ Average Rating: {avg_rating:.2f}/10
        🗳️  Total Votes: {total_votes:,}
        🔍 Data Sources: {len(dataframes)}
        
        📅 Data Period: 1890-2020
        🎭 Genre Coverage: {len(dataframes.get('genre_analysis', pd.DataFrame()).columns) if 'genre_analysis' in dataframes else 0} metrics
        📊 Decade Analysis: {len(dataframes.get('decade_trends', pd.DataFrame())) if 'decade_trends' in dataframes else 0} decades
        """
        
        ax4.text(0.1, 0.5, summary_text, fontsize=12, fontfamily='monospace',
                verticalalignment='center', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor='lightgray', alpha=0.8))
        
        # Add overall title
        fig.suptitle('Advanced Data Engineering Assignment - Task 4\nIMDb Data Visualization Dashboard', 
                    fontsize=20, fontweight='bold', y=0.98)
        
        # Save the integrated dashboard
        output_path = os.path.join(self.output_dir, "integrated_dashboard.png")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Integrated dashboard saved: {output_path}")
        return output_path
    
    def run_dashboard_creation(self):
        """Run the complete dashboard creation pipeline."""
        logger.info("=== Starting Integrated Dashboard Creation ===")
        
        try:
            # Load data
            dataframes = self.load_gold_data()
            
            if not dataframes:
                logger.error("No data loaded from gold layer")
                return {}
            
            # Create integrated dashboard
            dashboard_path = self.create_integrated_dashboard(dataframes)
            
            logger.info("=== Integrated Dashboard Creation Complete ===")
            return {'integrated_dashboard': dashboard_path}
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            raise

def main():
    """Main execution function."""
    try:
        dashboard_creator = IntegratedDashboardCreator()
        results = dashboard_creator.run_dashboard_creation()
        
        if results:
            print("✅ Integrated dashboard creation successful!")
            print("📊 Generated dashboard:")
            for dashboard_type, path in results.items():
                print(f"   - {dashboard_type}: {path}")
        else:
            print("❌ Dashboard creation failed")
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
