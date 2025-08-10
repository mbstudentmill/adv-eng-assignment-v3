# Advanced Data Engineering Assignment

## Project Overview
This project implements a complete data engineering pipeline for the Advanced Data Engineering coursework (LDSCI7229).

## Architecture
- **Task 1**: Data Pipeline (IMDb + NASA DONKI API) → GCS → BigQuery
- **Task 2**: Data Warehouse (Star Schema on IMDb data)
- **Task 3**: Batch Processing (PySpark aggregations)
- **Task 4**: Data Visualization (Looker Studio dashboard)

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. Set up GCP credentials
3. Run the pipeline: `python orchestration/main.py`

## Project Structure
```
├── ingestion/          # Data ingestion scripts
├── orchestration/      # Prefect flows and scheduling
├── warehouse/          # BigQuery DDL and queries
├── batch/             # PySpark batch processing
├── viz/               # Visualization outputs
├── diagrams/          # Architecture diagrams
├── docs/              # Task reports
└── requirements.txt    # Python dependencies
```

## Datasets
- **DB1**: IMDb full dataset (title.basics, ratings, principals, etc.)
- **DB2**: NASA DONKI Solar Flares API

## Technologies
- **Storage**: Google Cloud Storage (GCS)
- **Warehouse**: BigQuery
- **Processing**: PySpark
- **Orchestration**: Prefect
- **Visualization**: Looker Studio



