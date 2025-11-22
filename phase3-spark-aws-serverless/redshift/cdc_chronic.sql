CREATE TABLE cdc_chronic (
    year_start INT,
    year_end INT,
    location_abbr VARCHAR(10),
    location_desc VARCHAR(255),
    data_source VARCHAR(255),
    topic VARCHAR(255),
    question VARCHAR(500),
    data_value_unit VARCHAR(100),
    data_value_type VARCHAR(100),
    data_value DOUBLE PRECISION,
    data_value_alt DOUBLE PRECISION,
    low_confidence_limit DOUBLE PRECISION,
    high_confidence_limit DOUBLE PRECISION,
    stratification_category1 VARCHAR(100),
    stratification1 VARCHAR(100),
    geolocation VARCHAR(200),
    location_id INT,
    topic_id VARCHAR(50),
    question_id VARCHAR(50),
    data_value_type_id VARCHAR(50),
    stratification_category_id1 VARCHAR(50),
    stratification_id1 VARCHAR(50),
    row_num INT NOT NULL
);

GRANT INSERT, SELECT ON TABLE cdc_chronic TO "IAMR:load_cdc_redshift-role-gxb2dfmz"

GRANT TRUNCATE, INSERT, SELECT ON TABLE cdc_chronic TO "IAMR:load_cdc_redshift-role-gxb2dfmz";

COPY cdc_chronic
FROM 's3://center-disease-control/processed/chronic/'
IAM_ROLE 'arn:aws:iam::008878757943:role/RedshiftS3LoadRole'
FORMAT AS PARQUET;
