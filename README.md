# CDC ETL Pipelines
The three CDC datasets power three portfolio pipelines: Phase I, II, III, and IV.

### CDC public-health dataset
A comprehensive CDC public-health dataset covering chronic disease indicators, heart disease mortality (2019–2021), and nutrition/obesity behaviors.
It brings together standardized state/county health metrics, age-adjusted mortality rates, and BRFSS-based lifestyle data to provide a unified view of chronic conditions, cardiovascular risk, and population health behaviors across the U.S.

Phase I runs a traditional Airflow ETL, ingesting all three CSVs for baseline cleaning and loading. 

Phase II uses a hybrid Lambda/EC2 Pandas workflow focused on the Nutrition dataset with automated validation.

Phase III utilizes a serverless Spark (Lambda/Glue) pipeline to process large-scale Chronic and Heart Disease datasets, enabling scalable transformation and analytics.

**Note:** The AWS project includes minimal EDA/analysis, as a separate Cloud dbt project already covers it:

[Cloud CDC dbt ETL Project](https://github.com/masabai/cloud_center_disease_etl_dbt)

Phase IV runs a Databricks Community Edition PySpark ETL, loading datasets to Unity Catalog volumes; runtime may appear longer due to a single-node, limited-resource environment.

### Phase I: Traditional CDC ETL Pipeline with Airflow DAG
Demonstrates a classic, local ETL workflow using Airflow, Pandas, Postgres, and Great Expectations(GX).
Dataset: Three CDC CSVs (~100 MB combined) contain chronic disease, heart disease, and nutrition metrics, serving as a baseline for all subsequent phases.

Flow chart: data.gov → E (local) → T (Pandas) → L (Postgres) → Validate (GX) → DAG -> Slack/Email notifications

![Phase I Pandas ETL with Airflow ](https://github.com/masabai/aws-center-disease-etl/raw/stable/phase1-pandas-dag/pandas_airflow_screenshots/etl_dag_graph.png)

Extract (E): Airflow extracts the three CDC CSV files and copies them into the local raw/ directory.

Transform (T): Pandas cleans, standardizes, and enriches the raw data into structured outputs stored in the processed/ directory.

Load (L): Airflow loads the processed data into the local Postgres database for downstream analysis.

Validate (V): Great Expectations(GX) validates schema, types, and quality checks using expectations stored locally.

Notify (N): Airflow sends Slack notifications on success or failure at the end of the DAG run.

Scheduling:
Pipeline orchestrated via Airflow DAG, configured in Desktop Docker. The pipeline dynamically reports success/failure via Slack notifications.

Testing:
Local pytest is used to validate the extract, transformations, and validation schema checks.

### Phase II: AWS CDC ETL Pipeline — Hybrid (Pandas)
Showcase end-to-end data engineering on AWS using only free-tier resources, combining hybrid (EC2 + Lambda)
Dataset: Using a small dataset (~39 MB) ensures a cost-effective workflow while allowing for a full demonstration of the architecture, orchestration, validation, and operational skills required to build production-ready pipelines efficiently.

Flow chart: data.gov-> E (s3)-> T(s3)-> L(s3/Athena)-> V(EC2)-> Step Functions -> SNS-> EventBridge
![Phase II Step Functions Pandas ETL](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/stepfunctions_pandas_etl.png)
*Step Functions orchestrate Lambda, EC2, S3, Athena, and validation in a hybrid ETL pipeline.*

Extract (E): AWS Lambda extracts the raw dataset from data.gov and stores it in s3://center-disease-control/raw/.
  - [Extract & Load CSV Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/extract_load_csv.png)

Transform (T): Pandas transformations performed inside Lambda: data cleaning, type conversions, enrichment.
Cleaned data written as Parquet files to s3://center-disease-control/processed/.
  - [Transform & Load CSV Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/transform_load_csv.png)

Load (L): Athena tables created on processed Parquet data for downstream queries. Processed dataset verified via Athena queries. Athena tables are created manually from processed S3 Parquet; in production, this step can be automated using Glue Crawlers or boto3 scripts.
  - [Load Table in Athena Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/load_table_athena.png)

Validate (V): Great Expectations (GX) runs on EC2 (hybrid model). The Lambda function invokes GX via AWS Systems Manager Run Command.
JSON result automatically saved to s3://center-disease-control/processed/validation/ for review.

  - [ETL on EC2 Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/etl_ec2_instance.png)  
  - [Verify GX Result Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/verify_gx_result.png)

Visualization: Data loaded into Amazon QuickSight (Quick Suite) for analysis.
  - [QuickSight Analysis Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/quicksuite_analysis.png)

Scheduling:
Pipeline orchestrated via AWS Step Functions and scheduled with EventBridge. Pipeline dynamically reports success/failure in Step Functions, with SNS notifications.

### Phase III — CDC ETL with Spark (Serverless)
Demonstrates how the same CDC datasets can be processed at scale using AWS Lambda + Glue Spark in a serverless architecture. For free-tier cost optimization, full Great Expectations validation is skipped — the focus is on scalable ETL patterns and orchestration.
Dataset: CDC Chronic and Heart Disease data (~60 MB combined), processed at scale for analytics.

#### Flow chart: raw CSVs → Extract (Lambda) → Transform/Load (Glue Spark) → Verify Redshift → Step Functions → SNS notifications

![Phase III Step Functions Spark ETL](https://github.com/masabai/aws-center-disease-etl/blob/stable/phase3-spark-serverless/spark_etl_screenshots/state_machine_graph.png)

Extract (E): Reuses the same Lambda function from Phase I.
- [Extract & Load CSV Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/master/phase2-pandas-hybrid/pandas_etl_screenshots/extract_load_csv.png)

Transform & Load (T/L): Handled by AWS Glue Job (Spark) for scalable processing, and verified in Redshift.

- [Glue Transform Run Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/stable/phase3-spark-serverless/spark_etl_screenshots/glue_transform_run.png)

- [Load to Redshift Lambda Log Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/stable/phase3-spark-serverless/spark_etl_screenshots/load_redshift_lambda_log.png)

Verify: Check that the transformed data has been correctly loaded into Redshift.  

- [Verify Rows in Redshift Screenshot](https://github.com/masabai/aws-center-disease-etl/blob/stable/phase3-spark-serverless/spark_etl_screenshots/verify_rows_redshift.png)

Note: Validation (V): Great Expectations(GX) step skipped, already included in Phase I and II — Glue and EC2 Spark validation can be cost-heavy for the free tier.

Scheduling:
Pipeline orchestrated via AWS Step Functions and scheduled with EventBridge. Pipeline dynamically reports success/failure in Step Functions, with SNS notifications.

### Phase IV: Basic CDC ETL with PySpark in Databricks Community Edition
Re-create the CDC ETL pipeline inside a managed Spark environment using Databricks Community Edition (DBCE).
Showcase PySpark transformation, data validation with Great Expectations (GX), loading processed datasets to Unity Catalog volumes, and scheduling with Databricks Jobs.

The runtime may appear longer in the job run screenshot because DBCE has single-node clusters with limited resources. This environment is intended for learning and prototyping, so performance does not reflect full-scale production Spark clusters.

**Note:**
Databricks CE does not provide access to /tmp, FileStore, or Delta tables; all data must be read from S3. To avoid unexpected AWS charges, minimal EDA/analysis is included — a full exploration is already handled in the separate Cloud dbt project.

##### Flow chart: data.gov-> (s3)-> E (DBCE) -> T(DBCE)-> V(DBCE)->L(DBCE)-> job runs
![Phase IV Databricks Job Run](https://raw.githubusercontent.com/masabai/aws-center-disease-etl/blob/stable/phase4-spark-databrisks/spark_databricks_etl_screenshot/databricks_jobrun_cdc_etl.png)
