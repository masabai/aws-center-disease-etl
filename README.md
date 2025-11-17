# CDC ETL Pipelines
The three CDC datasets power three portfolio pipelines: Phase I, II, III

### CDC public-health dataset
A comprehensive CDC public-health dataset covering chronic disease indicators, 
heart-disease mortality (2019–2021), and nutrition/obesity behaviors.
It brings together standardized state/county health metrics, age-adjusted mortality rates, and BRFSS-based lifestyle data to provide a unified view of chronic conditions, cardiovascular risk, 
and population health behaviors across the U.S.

Phase I runs a traditional Airflow ETL ingesting all three CSVs for baseline cleaning and loading; 
Phase II uses a hybrid Lambda/EC2 Pandas workflow focused on the Nutrition dataset with automated validation; and 
Phase III applies a serverless Spark (Lambda/Glue) pipeline to process large-scale Chronic and Heart Disease datasets for scalable transformation and analytics.

### Phase I: Traditional CDC ETL Pipeline with Airflow Dag
Dataset: CDC Data (3 CSVs, ~100 MB) -- Work in progress

### Phase II: AWS CDC ETL Pipeline — Hybrid (Pandas)
Showcase end-to-end data engineering on AWS using only free-tier resources, combining hybrid (EC2 + Lambda)
Dataset: Using a small dataset (~39 MB) ensures a cost-effective workflow while allowing full demonstration of architecture,
orchestration, validation, and operational skills to build production-ready pipelines efficiently.
-- flow chart here data.gov-> E (s3)-> T(s3)-> L(s3/Athena)-> V(EC2)-> Step Functions -> SNS-> EventBridge

Extract (E): AWS Lambda extracts raw dataset from data.gov and stores it in s3://center-disease-control/raw/.
![Extract & Load CSV](phase2-pandas-hybrid/pandas_etl_screenshots/extract_load_csv.png)

**** clickable *****
[Extract & Load CSV Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/extract_load_csv.png)

Transform (T): Pandas transformations performed inside Lambda: data cleaning, type conversions, enrichment.
Cleaned data written as Parquet files to s3://center-disease-control/processed/.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\transform_load_csv.png

Load (L): Athena tables created on processed Parquet data for downstream queries. Processed dataset verified via Athena queries.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\load_table_athena.png
https://github.com/yourusername/CenterDiseaseControl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/load_table_athena.png
Validate (V): Great Expectations (GX) runs on EC2 (hybrid model). Lambda function invokes GX via SSM Run Command.
Validation JSON automatically saved to s3://center-disease-control/processed/validation/ for review.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\etl_ect_instance.png
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\load_validation_s3.png
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\verify_gx_result.png

Visualization: Data loaded into Amazon QuickSight (Quick Suite) for analysis.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\quicksuite_analysis.png

Testing:
Local pytest used for validation of transformations and schema checks.

Scheduling:
Pipeline orchestrated via AWS Step Functions and scheduled with EventBridge. Pipeline dynamically reports success/failure in Step Functions, with SNS notifications
-- will paste screenshot, state machine HERE
-- the rest will refer to screenshot link under each tep

### Phase III — CDC ETL with Spark (Serverless)
Dataset: CDC Nutrition Data (2 CSVs, ~60 MB)
Objective: Extend the same pipeline to handle larger data using AWS Glue (Spark-based).
Purpose: Demonstrate Spark ETL orchestration on AWS in a cost-optimized way.
Architecture: Fully serverless (Lambda + Glue + Step Functions + SNS)

Pipeline Overview
Extract (E): Reuses the same Lambda function from Phase I.
Transform & Load (T/L): Handled by AWS Glue Job (Spark) for scalable processing.
Validation (V): GX step skipped — Glue and EC2 Spark validation can be cost-heavy for free tier.
Verification: Data loaded into Amazon Redshift.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\quicksuite_analysis.png

Testing:
Local pytest used for validation of transformations and schema checks.

Scheduling:
Pipeline orchestrated via AWS Step Functions and scheduled with EventBridge. Pipeline dynamically reports success/failure in Step Functions, with SNS notifications
-- will paste screenshot, state machine HERE

# TBD:
Add troubleshooting notes and lessons learned
Lambda Layers
Move to EC2
SSH login issues from PowerShell
Fixed: Adjusted .pem key permissions
Step Functions
EC2 spark install
etc
