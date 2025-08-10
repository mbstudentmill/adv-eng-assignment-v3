# Setup Instructions - Advanced Data Engineering Assignment

## Overview

This document provides setup instructions for running the Advanced Data Engineering assignment locally.

## Prerequisites

### Required Software
- Python 3.8 or higher
- Git
- Internet connection

### Optional (for cloud features)
- Google Cloud Platform account
- Google Cloud SDK

## Installation

### 1. Clone Repository
```bash
git clone https://github.com/mbstudentmill/adv-eng-assignment-v3.git
cd adv-eng-assignment-v3
```

### 2. Create Virtual Environment
```bash
python -m venv venv

# Activate on Windows:
venv\Scripts\activate
# Activate on macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
cp env.template .env
# Edit .env with your NASA API key
```

### 5. NASA API Key
1. Visit https://api.nasa.gov/
2. Sign up for free account
3. Generate API key
4. Add to .env file: `NASA_API_KEY=your_key_here`

## Testing

### Run Test Suite
```bash
python test_assignment.py
```

### Expected Output
```
🚀 Starting Advanced Data Engineering Assignment Test Suite
==============================================================
🧪 Running: Environment Setup
✅ Python 3.9.7
✅ pandas
✅ pyspark
✅ pyarrow
✅ prefect
✅ requests
✅ matplotlib
✅ seaborn
✅ PASS: Environment Setup

... (additional tests) ...

📊 TEST RESULTS SUMMARY
✅ Passed: 10/10
❌ Failed: 0/10
📈 Success Rate: 100.0%
```

## Running the Assignment

### Complete Pipeline
```bash
python orchestration/main.py
```

### Individual Components
```bash
# IMDb ingestion
python ingestion/imdb/imdb_ingestion.py

# NASA ingestion
python ingestion/nasa/nasa_ingestion.py

# PySpark processing
python batch/pyspark_batch.py

# Visualizations
python viz/create_visualizations.py
```

## Output Structure

### Data Flow
1. Bronze Layer: Raw data
2. Silver Layer: Cleaned data
3. Gold Layer: Aggregated data
4. Visualizations: Charts and dashboards

### File Locations
- Raw data: `data/bronze/`
- Processed data: `data/silver/`
- Aggregated data: `data/gold/`
- Charts: `viz/output/`
- Diagrams: `diagrams/`

## Troubleshooting

### Common Issues

#### Import Errors
```bash
source venv/bin/activate
pip install -r requirements.txt
```

#### NASA API Issues
```bash
echo $NASA_API_KEY
curl "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY"
```

#### PySpark Issues
```bash
java -version
export JAVA_HOME=/path/to/java
```

#### Memory Issues
```bash
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g
```

## Advanced Features

### Cloud Integration
1. Set up GCP project:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. Create GCS bucket:
   ```bash
   gsutil mb gs://your-bucket-name
   ```

3. Update environment:
   ```bash
   GCP_PROJECT_ID=your_project_id
   GCS_BUCKET_NAME=your_bucket_name
   ```

### Custom Data Sources
- Modify `ingestion/imdb/imdb_ingestion.py` for different datasets
- Update `ingestion/nasa/nasa_ingestion.py` for other APIs
- Adapt pipeline in `orchestration/main.py`

## Assignment Components

### Core Modules
- Ingestion: Multi-source data collection
- Transformation: Data cleaning and structuring
- Storage: Bronze/silver/gold architecture
- Processing: PySpark batch analytics
- Visualization: Charts and dashboards
- Orchestration: Workflow management

### Learning Objectives
- Multi-source data ingestion
- Scalable data storage design
- Data quality validation
- Batch processing with PySpark
- Data visualization
- Workflow orchestration

## Next Steps

After setup:
1. Explore the code structure
2. Run the complete pipeline
3. Analyze generated visualizations
4. Experiment with modifications
