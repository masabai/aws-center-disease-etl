CREATE EXTERNAL TABLE IF NOT EXISTS heart_cleaned (
  year INT,
  location_abbr STRING,
  location_desc STRING,
  geographic_level STRING,
  data_source STRING,
  class STRING,
  topic STRING,
  data_value DOUBLE,
  data_value_unit STRING,
  data_value_type STRING,
  stratification_category1 STRING,
  stratification1 STRING,
  stratification_category2 STRING,
  stratification2 STRING,
  topic_id TIMESTAMP,
  location_id INT,
  y_lat DOUBLE,
  x_lon DOUBLE,
  georeference STRING,
  row_num INT
)
STORED AS PARQUET
LOCATION 's3://center-disease-control/processed/heart/';
