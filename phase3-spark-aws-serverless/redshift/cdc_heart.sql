CREATE TABLE cdc_heart (
    year INT,
    location_abbr VARCHAR(10),
    location_desc VARCHAR(255),
    geographic_level VARCHAR(100),
    data_source VARCHAR(255),
    class VARCHAR(255),
    topic VARCHAR(255),
    data_value DOUBLE PRECISION,
    data_value_unit VARCHAR(50),
    data_value_type VARCHAR(100),
    stratification_category1 VARCHAR(100),
    stratification1 VARCHAR(100),
    stratification_category2 VARCHAR(100),
    stratification2 VARCHAR(100),
    topic_id TIMESTAMP,
    location_id INT,
    y_lat DOUBLE PRECISION,
    x_lon DOUBLE PRECISION,
    georeference VARCHAR(100),
    row_num INT NOT NULL
);

GRANT INSERT, SELECT ON TABLE cdc_heart TO "IAMR:load_cdc_redshift-role-gxb2dfmz"

GRANT TRUNCATE, INSERT, SELECT
ON TABLE cdc_heart TO "IAMR:load_cdc_redshift-role-gxb2dfmz";

# Test and delete table after that
COPY cdc_heart
FROM 's3://center-disease-control/processed/heart/'
IAM_ROLE 'arn:aws:iam::008878757943:role/RedshiftS3LoadRole'
FORMAT AS PARQUET;

