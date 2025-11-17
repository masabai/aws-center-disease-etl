CREATE EXTERNAL TABLE IF NOT EXISTS chronic_cleaned (
  year_start INT,
  year_end INT,
  location_abbr STRING,
  location_desc STRING,
  data_source STRING,
  topic STRING,
  question STRING,
  data_value_unit STRING,
  data_value_type STRING,
  data_value DOUBLE,
  data_value_alt DOUBLE,
  low_confidence_limit DOUBLE,
  high_confidence_limit DOUBLE,
  stratification_category1 STRING,
  stratification1 STRING,
  geolocation STRING,
  location_id INT,
  topic_id STRING,
  question_id STRING,
  data_value_type_id STRING,
  stratification_category_id1 STRING,
  stratification_id1 STRING,
  row_num INT
)
STORED AS PARQUET
LOCATION 's3://center-disease-control/processed/chronic/';
