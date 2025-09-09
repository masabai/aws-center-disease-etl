# CDC ETL and EDA Projects

Overview
This repository contains two complementary projects using publicly available CDC health datasets:
PySpark ETL Project – Demonstrates extraction, transformation, and loading (ETL) of large-scale CDC datasets.
Pandas EDA Project – Performs exploratory data analysis (EDA) on the CDC Chronic Disease Indicators dataset to derive insights.


## PySpark ETL
Showcase the ability to process large datasets with PySpark.
Focus on data ingestion, cleaning, and loading, rather than statistical analysis.
Multiple CDC datasets are included to increase row count and data complexity, ensuring a realistic ETL demonstration.
No deep EDA is performed here, as the goal is pipeline design and scalability.

## Pandas EDA
Perform detailed exploratory data analysis on a single CDC dataset (Chronic Disease Indicators).
Inspect data distributions, missing values, and relationships between variables.
Demonstrates ability to generate insights using Python and Pandas for smaller-scale, analytic-focused tasks.

Datasets
CDC1: Chronic Disease Indicators (main dataset for Pandas EDA)
CDC2: Heart Disease Mortality (included in PySpark ETL)
CDC3: Nutrition & Obesity Behavioral Risk Factor Surveillance (included in PySpark ETL)

All datasets are publicly available from the CDC and stored in CSV format under /data/raw.
Structure
/data/raw      # raw CSV files
/cdc_etl       # Pandas/PySpark ETL scripts

### Key Skills Demonstrated
PySpark: reading large CSVs, schema inference, basic transformations, and loading
Pandas: exploratory data analysis, grouping, missing data handling, summary statistics

