# Advanced Data Engineering Assignment

**Course Code:** LDSCI7229  
**Assessment:** AE1 - Course Work  
**Student:** Anonymous (Academic Submission)  
**Deadline:** 1pm 11 August 2025  

## �� Project Overview

This implementation includes a data engineering solution covering four main tasks as required by the assignment:

1. **Data Pipeline Development** - End-to-end data ingestion and processing from multiple sources
2. **Data Warehouse Design** - Optimized schema with performance features and testing
3. **Batch Processing Implementation** - Scalable PySpark processing with data quality validation
4. **Data Visualization** - Interactive dashboards and charts with comprehensive testing

## Architecture Overview

### **Data Sources**
- **IMDb Dataset (DB1)**: Complete data dump with foreign key relationships
- **NASA DONKI API (DB2)**: Solar flare and space weather data

### **Technology Stack**
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Storage**: Google Cloud Storage (GCS) with bronze/silver/gold zones
- **Warehouse**: BigQuery with optimized schema design
- **Processing**: PySpark 4.0.0 for batch processing
- **Orchestration**: Prefect for workflow management
- **Visualization**: Matplotlib, Seaborn, and integrated dashboards

### **Data Flow Architecture**
```
IMDb Data + NASA API → GCS Bronze Zone → GCS Silver Zone → GCS Gold Zone
                                    ↓
                            PySpark Processing → BigQuery Warehouse → Visualization
```

## Complete Repository Structure

*Repository URL:* https://github.com/mbstudentmill/adv-eng-assignment-v3

```
adv-eng-assignment-v3/
│
├── CORE TASK IMPLEMENTATIONS
├── batch/
│   └── pyspark_batch.py                    # Task 3: PySpark batch processing engine
│
├── diagrams/
│   ├── generate_diagrams.py                # Automated diagram generation script  
│   ├── pipeline.png                        # Task 1: Data pipeline architecture
│   └── schema.png                          # Task 2: Star schema warehouse design
│
├── ingestion/
│   ├── imdb/
│   │   └── imdb_ingestion.py              # Task 1: IMDb data ingestion logic
│   └── nasa/
│       └── nasa_ingestion.py              # Task 1: NASA DONKI API integration
│
├── orchestration/
│   └── main.py                            # Task 1: Prefect workflow orchestration
│
├── viz/
│   ├── output/                            # Task 4: Generated visualizations
│   │   ├── decade_analysis.png            # Decade trends analysis
│   │   ├── genre_performance.png          # Genre performance charts (optimized)
│   │   ├── integrated_dashboard.png       # Combined dashboard view  
│   │   ├── rating_trends.png              # Rating trends over time
│   │   └── summary_dashboard.png          # Summary metrics overview
│   ├── create_integrated_dashboard.py     # Task 4: Dashboard creation engine
│   └── create_visualizations.py           # Task 4: Individual chart generation
│
├── warehouse/
│   └── ddl/
│       ├── create_warehouse.sql           # Task 2: Main warehouse schema
│       └── create_warehouse_fixed.sql     # Task 2: Optimized warehouse schema
│
├── TASK COMPLETION DOCUMENTATION  
├── TASK1_COMPLETION_REPORT.md             # Task 1: Pipeline implementation report
├── TASK2_COMPLETION_REPORT.md             # Task 2: Warehouse design report
├── TASK3_COMPLETION_REPORT.md             # Task 3: Batch processing report
├── TASK4_COMPLETION_REPORT.md             # Task 4: Visualization report
├── FINAL_SUBMISSION_CHECKLIST.md          # Assignment completion verification
├── README.md                              # Project overview and setup guide
│
├── COMPREHENSIVE TESTING FRAMEWORK
├── test_assignment.py                     # Testing suite (10 test categories)
├── test_bigquery_connection.py            # BigQuery connectivity validation
├── test_complete_pipeline.py              # End-to-end pipeline testing  
├── test_gcs_connection.py                 # Google Cloud Storage testing
├── test_warehouse_performance.py          # Warehouse performance validation
│
├── CONFIGURATION & UTILITIES
├── requirements.txt                       # Python dependencies
├── config.py / config_public.py          # Configuration management system
├── env.template                           # Environment variables template
├── setup.py                              # Project setup and installation
├── setup_instructions.md                 # Detailed setup guide
├── data_quality_checks.py                # Data validation framework
├── gcs_config_public.py                  # GCS configuration management
│
└── DEPLOYMENT & MANAGEMENT SCRIPTS
    ├── create_warehouse_tables.py         # Automated warehouse deployment
    ├── execute_task2_warehouse.py         # Task 2 execution coordinator
    ├── add_imdb_files_to_gcs.py          # IMDb data upload automation
    ├── upload_imdb_data_to_gcs.py        # IMDb GCS upload management  
    ├── upload_nasa_data_to_gcs.py        # NASA data GCS upload management
    ├── list_gcs_buckets.py               # GCS bucket inspection utility
    ├── list_gcs_contents.py              # GCS content management utility
    ├── setup_gcs_folders.py              # GCS folder structure automation
    └── fix_gcs_folders.py                # GCS folder repair utility
```

## Task Completion Reports

The implementation includes documentation of the approach and challenges for each task:

- **[Task 1 Completion Report](TASK1_COMPLETION_REPORT.md)** - Data pipeline development 
- **[Task 2 Completion Report](TASK2_COMPLETION_REPORT.md)** - Data warehouse design 
- **[Task 3 Completion Report](TASK3_COMPLETION_REPORT.md)** - PySpark processing 
- **[Task 4 Completion Report](TASK4_COMPLETION_REPORT.md)** - Data visualization 

## Quick Start

### **Prerequisites**
- Python 3.8+
- Java 17+ (for PySpark)
- Google Cloud Platform account
- GCS bucket with appropriate permissions

### **Installation**
```bash
# Clone the repository
git clone <repository-url>
cd adv-data-eng-assignment

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env.template .env
# Edit .env with your GCP credentials and bucket details
```

### **Configuration**
1. Copy `env.template` to `.env`
2. Update the following variables:
   - GCP project ID
   - GCS bucket name
   - Zone paths (bronze, silver, gold)
   - Credentials path

### **Running the Project**
```bash
# Test the complete implementation
python test_assignment.py

# Run individual components
python ingestion/imdb/imdb_ingestion.py
python batch/pyspark_batch.py
python viz/create_visualizations.py
```

## Testing Framework

The implementation includes a testing framework (`test_assignment.py`) that validates:

- **Data Ingestion**: IMDb and NASA API connectivity
- **Data Processing**: PySpark batch processing functionality
- **Data Warehouse**: BigQuery connection and query performance
- **Data Quality**: Validation of data integrity and completeness
- **Visualization**: Chart generation and output validation

The test suite covers all assignment components and can be run with:
```bash
python test_assignment.py
```

## Generated Outputs

### **Visualizations (Task 4)**
- `decade_analysis.png` - Decade-based rating trends analysis
- `genre_performance.png` - Genre performance comparison
- `integrated_dashboard.png` - Comprehensive dashboard view
- `rating_trends.png` - Rating trends over time
- `summary_dashboard.png` - Summary metrics dashboard

### **Architecture Diagrams**
- `pipeline.png` - Complete data pipeline architecture
- `schema.png` - Initial data warehouse schema
- `schema_updated.png` - Optimized final schema

## Key Implementation Files

### **Batch 5 (Already Submitted)**
- **Core Implementation**: `ingestion/`, `orchestration/`, `warehouse/`, `batch/`, `viz/`
- **Testing**: `test_assignment.py` with comprehensive validation
- **Documentation**: Task completion reports and architecture diagrams

### **Batch 6 (Current Submission)**
- **GCS Management**: Data upload and folder management scripts
- **Testing Frameworks**: Additional testing components for validation
- **Utilities**: Setup, data quality, and table management scripts

## Assignment Requirements

For detailed assignment criteria and grading information, see [ASSIGNMENT_REQUIREMENTS.md](ASSIGNMENT_REQUIREMENTS.md).

## What I Accomplished

### **Task 1: Data Pipeline Development**
The solution includes a multi-source data ingestion pipeline that handles both IMDb TSV.GZ datasets and NASA DONKI API data. The pipeline includes data cleaning, transformation, and storage in GCS bronze/silver/gold zones with error handling and retry logic.

### **Task 2: Data Warehouse Design**
The solution includes a star schema data warehouse in BigQuery with performance optimizations including partitioning by year and clustering by genre. The warehouse supports complex analytical queries and includes optimization techniques.

### **Task 3: Batch Processing Implementation**
The solution includes scalable PySpark batch processing that handles large-scale data aggregation from the IMDb dataset. The implementation includes memory optimization, adaptive execution, and data quality validation.

### **Task 4: Data Visualization**
The solution includes visualizations using Python libraries (Matplotlib and Seaborn) that present insights from the processed data. The implementation includes both individual charts and an integrated dashboard.

## Challenges and Solutions

During the implementation, several challenges were encountered:

1. **PySpark Environment Setup**: Java version compatibility issues resolved by ensuring Java 17 installation
2. **GCS Authentication**: Credential management challenges solved with proper environment variable configuration
3. **Data Quality Validation**: Implemented comprehensive validation framework to ensure data integrity
4. **Performance Optimization**: Applied partitioning and clustering strategies for warehouse performance

## Project Status

- **Task 1**: Complete with testing
- **Task 2**: Complete with performance optimization
- **Task 3**: Complete with scalable processing
- **Task 4**: Complete with visualizations

All tasks have been implemented according to the assignment specifications with testing and documentation.

## Notes

- This project is created for academic assessment purposes only
- All code and documentation are original work created for the Advanced Data Engineering course (LDSCI7229)
- The implementation includes practical application of data engineering principles
- Testing ensures all components function correctly
- All files are sanitized and ready for academic submission
