#!/usr/bin/env python3
"""
Diagram Generation Script for Advanced Data Engineering Assignment
Generates pipeline.png and schema.png diagrams required for deliverables.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, ConnectionPatch
import numpy as np
from typing import List, Tuple
import os

class AssignmentDiagramGenerator:
    """Generates professional diagrams for the assignment."""
    
    def __init__(self):
        self.colors = {
            'source': '#FF6B6B',      # Red for data sources
            'process': '#4ECDC4',     # Teal for processing
            'storage': '#45B7D1',     # Blue for storage
            'warehouse': '#96CEB4',   # Green for warehouse
            'viz': '#FFEAA7',         # Yellow for visualization
            'text': '#2D3436'         # Dark text
        }
        
    def create_pipeline_diagram(self, output_path: str = "pipeline.png"):
        """Create the data pipeline architecture diagram (Task 1)."""
        fig, ax = plt.subplots(1, 1, figsize=(16, 10))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 8)
        ax.axis('off')
        
        # Title
        ax.text(5, 7.5, 'Data Pipeline Architecture', 
                fontsize=20, fontweight='bold', ha='center', color=self.colors['text'])
        
        # Data Sources (Row 1)
        self._draw_box(ax, 1, 6, 2, 1, 'IMDb Dataset\n(DB1)', self.colors['source'])
        self._draw_box(ax, 7, 6, 2, 1, 'NASA DONKI\n(DB2)', self.colors['source'])
        
        # Prefect Orchestration
        self._draw_box(ax, 4.5, 5, 1, 1, 'Prefect\nOrchestration', self.colors['process'])
        
        # Data Lake Zones
        self._draw_box(ax, 1, 3.5, 2, 1, 'GCS Bronze\n(Raw Data)', self.colors['storage'])
        self._draw_box(ax, 4, 3.5, 2, 1, 'GCS Silver\n(Cleaned)', self.colors['storage'])
        self._draw_box(ax, 7, 3.5, 2, 1, 'GCS Gold\n(Aggregated)', self.colors['storage'])
        
        # BigQuery Warehouse
        self._draw_box(ax, 4, 2, 2, 1, 'BigQuery\nData Warehouse', self.colors['warehouse'])
        
        # Processing
        self._draw_box(ax, 1, 0.5, 2, 1, 'PySpark\nBatch Processing', self.colors['process'])
        self._draw_box(ax, 7, 0.5, 2, 1, 'Looker Studio\nVisualization', self.colors['viz'])
        
        # Arrows
        self._draw_arrow(ax, 2, 6.5, 4.5, 5.5)  # IMDb to Prefect
        self._draw_arrow(ax, 8, 6.5, 4.5, 5.5)  # NASA to Prefect
        self._draw_arrow(ax, 4.5, 5, 2, 4)      # Prefect to Bronze
        self._draw_arrow(ax, 2, 4, 5, 4)        # Bronze to Silver
        self._draw_arrow(ax, 5, 4, 8, 4)        # Silver to Gold
        self._draw_arrow(ax, 5, 3.5, 5, 2.5)    # Silver to BigQuery
        self._draw_arrow(ax, 5, 2, 2, 1)        # BigQuery to PySpark
        self._draw_arrow(ax, 3, 1, 8, 1)        # PySpark to Gold
        self._draw_arrow(ax, 8, 1, 8, 1.5)      # Gold to Looker Studio
        
        # Legend
        legend_elements = [
            patches.Patch(color=self.colors['source'], label='Data Sources'),
            patches.Patch(color=self.colors['process'], label='Processing'),
            patches.Patch(color=self.colors['storage'], label='Storage'),
            patches.Patch(color=self.colors['warehouse'], label='Warehouse'),
            patches.Patch(color=self.colors['viz'], label='Visualization')
        ]
        ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(0.98, 0.98))
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Pipeline diagram saved to {output_path}")
    
    def create_schema_diagram(self, output_path: str = "schema.png"):
        """Create the data warehouse schema diagram (Task 2)."""
        fig, ax = plt.subplots(1, 1, figsize=(14, 10))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 8)
        ax.axis('off')
        
        # Title
        ax.text(5, 7.5, 'IMDb Data Warehouse - Star Schema', 
                fontsize=20, fontweight='bold', ha='center', color=self.colors['text'])
        
        # Fact Table (Center)
        self._draw_box(ax, 4.5, 4, 1, 1.5, 'fact_title_rating\n(Fact Table)', self.colors['warehouse'])
        
        # Dimension Tables
        self._draw_box(ax, 1, 6, 1.5, 1, 'dim_title\n(Title Info)', self.colors['storage'])
        self._draw_box(ax, 7.5, 6, 1.5, 1, 'dim_person\n(Person Info)', self.colors['storage'])
        self._draw_box(ax, 1, 1, 1.5, 1, 'dim_genre\n(Genre Info)', self.colors['storage'])
        self._draw_box(ax, 7.5, 1, 1.5, 1, 'dim_region\n(Region Info)', self.colors['storage'])
        self._draw_box(ax, 4.5, 1, 1, 1, 'dim_date\n(Date Info)', self.colors['storage'])
        
        # Bridge Table
        self._draw_box(ax, 4.5, 6, 1, 1, 'bridge_title_genre\n(Bridge Table)', self.colors['process'])
        
        # Arrows (relationships)
        self._draw_arrow(ax, 2.5, 6.5, 4.5, 5)      # dim_title to fact
        self._draw_arrow(ax, 7.5, 6.5, 4.5, 5)      # dim_person to fact
        self._draw_arrow(ax, 2.5, 1.5, 4.5, 4)      # dim_genre to fact
        self._draw_arrow(ax, 7.5, 1.5, 4.5, 4)      # dim_region to fact
        self._draw_arrow(ax, 5, 2, 4.5, 4)          # dim_date to fact
        self._draw_arrow(ax, 2.5, 6.5, 4.5, 6.5)    # dim_title to bridge
        self._draw_arrow(ax, 4.5, 6.5, 5.5, 6.5)    # bridge to dim_genre
        
        # Key Fields
        ax.text(5, 3, 'Key Fields:\n• tconst (title ID)\n• nconst (person ID)\n• averageRating\n• numVotes\n• startYear\n• genre', 
                fontsize=10, ha='center', va='center', 
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
        
        # Partitioning Info
        ax.text(5, 0.5, 'Partitioning: startYear | Clustering: tconst, genre', 
                fontsize=12, ha='center', va='center', 
                bbox=dict(boxstyle="round,pad=0.3", facecolor=self.colors['warehouse'], alpha=0.3))
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Schema diagram saved to {output_path}")
    
    def _draw_box(self, ax, x, y, width, height, text, color):
        """Draw a box with text."""
        box = FancyBboxPatch((x, y), width, height, 
                             boxstyle="round,pad=0.1", 
                             facecolor=color, 
                             edgecolor='black', 
                             linewidth=1)
        ax.add_patch(box)
        ax.text(x + width/2, y + height/2, text, 
                ha='center', va='center', fontsize=9, fontweight='bold')
    
    def _draw_arrow(self, ax, x1, y1, x2, y2):
        """Draw an arrow between two points."""
        arrow = ConnectionPatch((x1, y1), (x2, y2), "data", "data",
                               arrowstyle="->", shrinkA=5, shrinkB=5,
                               mutation_scale=20, fc=self.colors['text'])
        ax.add_patch(arrow)

def main():
    """Generate all required diagrams."""
    generator = AssignmentDiagramGenerator()
    
    # Create diagrams directory if it doesn't exist
    os.makedirs('diagrams', exist_ok=True)
    
    # Generate pipeline diagram (Task 1)
    generator.create_pipeline_diagram("diagrams/pipeline.png")
    
    # Generate schema diagram (Task 2)
    generator.create_schema_diagram("diagrams/schema.png")
    
    print("✅ All diagrams generated successfully!")
    print("📁 Check the 'diagrams/' folder for:")
    print("   - pipeline.png (Task 1 architecture)")
    print("   - schema.png (Task 2 warehouse design)")

if __name__ == "__main__":
    main()



