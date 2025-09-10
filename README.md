# CDC Health Data ETL Portfolio

This portfolio demonstrates end-to-end Data Engineering skills using Pandas, PySpark, dbt, Great Expectations, and Airflow on real-world CDC health datasets (~500K rows total).
It includes two complementary ETL projects: Pandas-based and PySpark-based, both orchestrated via Airflow.

Project Structure
CdcDataPipeline/
├─ cdc_etl/
│  ├─ extract_spark.py
│  ├─ transform_spark.py
│  ├─ load_spark.py
│  ├─ pandas_etl.py
│  └─ temp/      # temporary scripts (ignored in Git)
├─ data/
│  ├─ raw/       # input CSVs (~500K rows)
│  └─ processed/ # output from ETL (DB / S3)
├─ dags/
│  ├─ pandas_etl_dag.py
│  └─ spark_etl_dag.py
├─ dbt/
│  ├─ models/    # staging & marts
│  └─ dbt_project.yml
├─ tests/        # Pytest for extract/transform/load functions
├─ requirements.txt
└─ README.md

## Pandas ETL Project

Flow: CSV / DB / JSON → Transform → Validate → Load
Orchestration: Full Airflow DAG
Validation: Great Expectations (GX) for tables/columns/nulls/duplicates
Testing: Pytest unit tests for extract, transform, and load functions
Load: DB staging (Postgres) + optional CSV → S3
dbt: Builds clean marts and aggregates
EDA & Charts: Summary statistics, distributions, and visualizations

## PySpark ETL Project

Flow: CSV → Transform → Load
ETL Scripts: Modular extract/transform/load
Testing: Pytest for Spark functions
Load: DB staging only (local file writes disabled due to Hadoop issues)
dbt: Marts from staging tables
Airflow DAG: Minimal DAG using SparkSubmitOperator

Performance Optimizations:
Caching/persisting reusable DataFrames
Partitioning/coalescing before writes
Column pruning & predicate pushdown
Broadcast joins to reduce shuffles

EDA & Charts: Convert small Spark samples to Pandas for visualizations

### Tech Stack
Data Processing: Pandas, PySpark
Orchestration: Apache Airflow (Dockerized)
Database: PostgreSQL (staging schema)
Testing: Pytest
Data Validation: Great Expectations (Pandas only)
Data Modeling / Transformations: dbt
Visualization: Matplotlib / Seaborn (EDA charts)
Version Control: Git / GitHub

### Portfolio Highlights

Realistic mini-enterprise DE environment on a personal laptop
Modular ETL scripts for both Pandas and PySpark
End-to-end Airflow DAG orchestration
dbt for T-layer transformations and marts
Spark performance-aware ETL
Clean separation between raw data, staging, and marts
Visualizations / Screenshots (optional)
DAG screenshot from Airflow UI
Example chart showing trends in CDC datasets
dbt lineage or marts preview

### How to Run
Clone repository
Install dependencies:
pip install -r requirements.txt
Start Airflow (Dockerized)
Trigger DAGs from Airflow UI
Inspect DB staging tables and dbt marts

                        ┌─────────────┐
                        │   Raw CSVs  │
                        │  (~500K)    │
                        └──────┬──────┘
                               │
      ┌────────────────────────┴────────────────────────┐
      │                                                 │
      ▼                                                 ▼
┌─────────────┐                                 ┌─────────────┐
│  Pandas ETL │                                 │ PySpark ETL │
│  (Local)    │                                 │ (Local Spark)│
└──────┬──────┘                                 └──────┬──────┘
       │                                               │
       │ Extract → Transform → Validate → Load         │ Extract → Transform → Load
       ▼                                               ▼
┌─────────────┐                                 ┌─────────────┐
│ Great Exp.  │                                 │ Validation  │
│ (Pandas)    │                                 │ skipped    │
└──────┬──────┘                                 └──────┬──────┘
       │                                               │
       │                                               │
       ▼                                               ▼
┌─────────────┐                                 ┌─────────────┐
│  Postgres   │                                 │  Postgres   │
│  Staging    │                                 │  Staging    │
└──────┬──────┘                                 └──────┬──────┘
       │                                               │
       │                                               │
       ▼                                               ▼
┌─────────────┐                                 ┌─────────────┐
│   dbt Marts │ <────────────── Airflow DAG ───>│   dbt Marts │
│ (Aggregates │                                 │ (Aggregates │
│ & Cleaned)  │                                 │ & Cleaned)  │
└─────────────┘                                 └─────────────┘
       │
       │ (Optional)
       ▼
┌─────────────┐
│ CSV → S3    │
│ (Pandas only) │
└─────────────┘
