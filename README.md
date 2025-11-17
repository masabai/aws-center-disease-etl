# CDC ETL Pipelines
The three CDC datasets power three portfolio pipelines: Phase I, II, and III.

### CDC public-health dataset
A comprehensive CDC public-health dataset covering chronic disease indicators, 
heart-disease mortality (2019–2021), and nutrition/obesity behaviors.
It brings together standardized state/county health metrics, age-adjusted mortality rates, and BRFSS-based lifestyle data to provide a unified view of chronic conditions, cardiovascular risk, 
and population health behaviors across the U.S.

Phase I runs a traditional Airflow ETL, ingesting all three CSVs for baseline cleaning and loading. 
Phase II uses a hybrid Lambda/EC2 Pandas workflow focused on the Nutrition dataset with automated validation; and 
Phase III utilizes a serverless Spark (Lambda/Glue) pipeline to process large-scale Chronic and Heart Disease datasets, enabling scalable transformation and analytics.

### Phase I: Traditional CDC ETL Pipeline with Airflow DAG
Dataset: CDC Data (3 CSVs, ~100 MB) -- Work in progress

### Phase II: AWS CDC ETL Pipeline — Hybrid (Pandas)
Showcase end-to-end data engineering on AWS using only free-tier resources, combining hybrid (EC2 + Lambda)
Dataset: Using a small dataset (~39 MB) ensures a cost-effective workflow while allowing for a full demonstration of the architecture, orchestration, validation, and operational skills required to build production-ready pipelines efficiently.
-- flow chart here data.gov-> E (s3)-> T(s3)-> L(s3/Athena)-> V(EC2)-> Step Functions -> SNS-> EventBridge

![Step Functions Pandas ETL](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/stepfunctions_pandas_etl.png)
*Step Functions orchestrate Lambda, EC2, S3, Athena, and validation in a hybrid ETL pipeline.*

Extract (E): AWS Lambda extracts raw dataset from data.gov and stores it in s3://center-disease-control/raw/.
- **Extract (E)** → AWS Lambda extracts the raw dataset from data.gov and stores it in S3.  
  - [Extract & Load CSV Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/extract_load_csv.png)

Transform (T): Pandas transformations performed inside Lambda: data cleaning, type conversions, enrichment.
Cleaned data written as Parquet files to s3://center-disease-control/processed/.
- **Transform (T)** → Pandas transformations inside Lambda: cleaning, type conversions, enrichment.  
  - [Transform & Load CSV Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/transform_load_csv.png)

Load (L): Athena tables created on processed Parquet data for downstream queries. Processed dataset verified via Athena queries.
- **Load (L)** → Athena tables created on processed Parquet data. Verified via queries.  
  - [Load Table in Athena Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/load_table_athena.png)

Validate (V): Great Expectations (GX) runs on EC2 (hybrid model). The Lambda function invokes GX via AWS Systems Manager Run Command.
Validation JSON automatically saved to s3://center-disease-control/processed/validation/ for review.
- **Validate (V)** → Great Expectations runs on EC2 (hybrid model). Lambda invokes GX via SSM. Validation JSON saved to S3.  
  - [ETL on EC2 Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/etl_ec2_instance.png)  
  - [Verify GX Result Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/verify_gx_result.png)

Visualization: Data loaded into Amazon QuickSight (Quick Suite) for analysis.
- **Visualization** → Data loaded into Amazon QuickSight for analysis.  
  - [QuickSight Analysis Screenshot](phase2-pandas-hybrid/pandas_etl_screenshots/quicksuite_analysis.png)

Testing:
Local pytest is used for validating transformations and performing schema checks.

Scheduling:
Pipeline orchestrated via AWS Step Functions and scheduled with EventBridge. Pipeline dynamically reports success/failure in Step Functions, with SNS notifications.

### Phase III — CDC ETL with Spark (Serverless)
Dataset: CDC Nutrition Data (2 CSVs, ~60 MB)
Objective: Extend the same pipeline to handle larger data using AWS Glue (Spark-based).
Purpose: Demonstrate Spark ETL orchestration on AWS in a cost-optimized way.
Architecture: Fully serverless (Lambda + Glue + Step Functions + SNS)

Pipeline Overview
Extract (E): Reuses the same Lambda function from Phase I.
Transform & Load (T/L): Handled by AWS Glue Job (Spark) for scalable processing.
Validation (V): GX step skipped — Glue and EC2 Spark validation can be cost-heavy for the free tier.
Verification: Data loaded into Amazon Redshift.
-- CenterDiseaseControl\phase2-pandas-hybrid\pandas_etl_screenshots\quicksuite_analysis.png

Testing:
Local pytest is used for validating transformations and performing schema checks.

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
EC2 Spark install
etc
