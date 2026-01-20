CREATE EXTERNAL TABLE IF NOT EXISTS cdc_health_db.nutrition_cleaned (
    YearStart INT,
    YearEnd INT,
    LocationAbbr STRING,
    LocationDesc STRING,
    Datasource STRING,
    Class STRING,
    Question STRING,
    Data_Value_Type STRING,
    Data_Value FLOAT,
    Data_Value_Alt FLOAT,
    Low_Confidence_Limit FLOAT,
    High_Confidence_Limit FLOAT,
    Sample_Size INT,
    Total STRING,
    Age_Range STRING,
    Education STRING,
    Income_Range STRING,
    Race_Ethnicity STRING,
    GeoLocation STRING,
    ClassID STRING,
    TopicID STRING,
    QuestionID STRING,
    DataValueTypeID STRING,
    LocationID STRING,
    StratificationCategory1 STRING,
    Stratification1 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
-- Define how to parse CSV files (delimiter, quotes, headers, null handling)
LOCATION 's3://center-disease-control/processed/nutri'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'skip.header.line.count' = '1',
    'use.null.for.invalid.data' = 'true'
);