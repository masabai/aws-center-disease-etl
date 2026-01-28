# cdc_load_to_redshift.py
import boto3
import time

WORKGROUP_NAME = "default-workgroup"
DATABASE_NAME = "dev"
S3_BUCKET = "center-disease-control"
IAM_ROLE_ARN = "arn:aws:iam::008878757943:role/RedshiftS3LoadRole"

# Role to grant table access
RECIPIENT_ROLE = "IAMR:load_cdc_redshift-role-gxb2dfmz"

# Dataset → S3 folder mapping
DATASETS = {
    "chronic": "processed/chronic/",
    "heart": "processed/heart/"
}

# Dataset → Redshift table mapping
TABLES = {
    "chronic": "cdc_chronic",
    "heart": "cdc_heart"
}

# Table schemas
TABLE_SCHEMAS = {
    "cdc_chronic": """
        year_start INT, year_end INT, location_abbr VARCHAR(10), location_desc VARCHAR(255),
        data_source VARCHAR(255), topic VARCHAR(255), question VARCHAR(500),
        data_value_unit VARCHAR(100), data_value_type VARCHAR(100), data_value DOUBLE PRECISION,
        data_value_alt DOUBLE PRECISION, low_confidence_limit DOUBLE PRECISION,
        high_confidence_limit DOUBLE PRECISION, stratification_category1 VARCHAR(100),
        stratification1 VARCHAR(100), geolocation VARCHAR(200), location_id INT,
        topic_id VARCHAR(50), question_id VARCHAR(50), data_value_type_id VARCHAR(50),
        stratification_category_id1 VARCHAR(50), stratification_id1 VARCHAR(50), row_num INT NOT NULL
    """,
    "cdc_heart": """
        year INT, location_abbr VARCHAR(10), location_desc VARCHAR(255), geographic_level VARCHAR(100),
        data_source VARCHAR(255), class VARCHAR(255), topic VARCHAR(255), data_value DOUBLE PRECISION,
        data_value_unit VARCHAR(50), data_value_type VARCHAR(100), stratification_category1 VARCHAR(100),
        stratification1 VARCHAR(100), stratification_category2 VARCHAR(100), stratification2 VARCHAR(100),
        topic_id TIMESTAMP, location_id INT, y_lat DOUBLE PRECISION, x_lon DOUBLE PRECISION,
        georeference VARCHAR(100), row_num INT NOT NULL
    """
}

# Redshift Data API client
client = boto3.client("redshift-data", region_name="us-west-2")


def wait_for_statement(stmt_id):
    """
    Poll Redshift Data API until a statement finishes.

    Args:
        stmt_id (str): Redshift Data API statement ID.

    Raises:
        Exception: If the statement fails or is aborted.
    """
    while True:
        status = client.describe_statement(Id=stmt_id)
        if status["Status"] == "FINISHED":
            return
        if status["Status"] in ["FAILED", "ABORTED"]:
            raise Exception(status.get("Error"))
        time.sleep(2)


def lambda_handler(event, context):
    """
    Load CDC Chronic and Heart Disease datasets from S3 into Redshift Serverless.

    Steps:
    1. Create table if missing.
    2. Grant permissions to recipient role.
    3. Truncate table.
    4. Load Parquet files from S3.

    Args:
        event (dict): Lambda event payload (unused).
        context (LambdaContext): Lambda runtime context.

    Returns:
        dict: Status of the ETL operation.
    """
    for name, s3_path in DATASETS.items():
        table = TABLES[name]
        schema = TABLE_SCHEMAS[table]

        # Create table if missing
        res = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=f"CREATE TABLE IF NOT EXISTS {table} ({schema});"
        )
        wait_for_statement(res["Id"])

        # Grant table permissions
        grant_sql = f'GRANT TRUNCATE, INSERT, SELECT ON TABLE {table} TO "{RECIPIENT_ROLE}";'
        res = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=grant_sql
        )
        wait_for_statement(res["Id"])

        # Truncate table before reload
        res = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=f"TRUNCATE TABLE {table};"
        )
        wait_for_statement(res["Id"])

        # Load Parquet files from S3
        copy_sql = f"""
            COPY {table}
            FROM 's3://{S3_BUCKET}/{s3_path}'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """
        res = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=copy_sql
        )
        wait_for_statement(res["Id"])

    return {"status": "success"}
