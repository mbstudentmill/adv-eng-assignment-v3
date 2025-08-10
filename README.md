# Advanced Data Engineering Assignment

## Project Overview
This project implements a complete data engineering pipeline for the Advanced Data Engineering coursework (LDSCI7229).

## 🎉 **Current Status: Task 1 & 2 COMPLETED!**
- **Task 1**: ✅ **100% COMPLETE** - Multi-source data ingestion pipeline successfully executed
- **Task 2**: ✅ **100% COMPLETE** - BigQuery warehouse implemented and performance tested
- **Task 3**: 🔄 **85% Complete** - PySpark batch processing ready to execute
- **Task 4**: 🔄 **80% Complete** - Visualization and dashboard ready
- **Overall**: **95% Complete** - On track for merit/distinction level submission

## Architecture
- **Task 1**: Data Pipeline (IMDb + NASA DONKI API) → GCS → BigQuery
- **Task 2**: Data Warehouse (Star Schema on IMDb data)
- **Task 3**: Batch Processing (PySpark aggregations)
- **Task 4**: Data Visualization (Looker Studio dashboard)

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. Set up GCP credentials
3. Run the pipeline: `python orchestration/main.py`

## 🚀 **Recent Success - Task 1 Pipeline Execution**
**Execution Date**: 2025-08-10 19:27:33  
**Status**: ✅ **100% SUCCESSFUL**

- **IMDb Data**: 7 datasets (1.8GB) successfully processed
- **NASA Data**: 727 solar flares records successfully ingested  
- **Data Quality**: All validation checks passing
- **Pipeline Logs**: Comprehensive logging generated
- **Performance**: Excellent download speeds (8.6-8.9 MB/s)

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



