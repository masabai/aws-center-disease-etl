# load_cdc_rds.py
import json
import io
import boto3
import psycopg2
import pyarrow.parquet as pq

S3_BUCKET = "center-disease-control"
SECRET_NAME = "rds/cdc_postgres"
REGION = "us-west-2"

DATASETS = {
    "chronic": "processed/chronic/",
    "heart":   "processed/heart/"
}

TABLES = {
    "chronic": "cdc_chronic",
    "heart":   "cdc_heart"
}

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
        topic_id VARCHAR(50), location_id INT, y_lat DOUBLE PRECISION, x_lon DOUBLE PRECISION,
        georeference VARCHAR(100), row_num INT NOT NULL
    """
}


def get_rds_credentials():
    client = boto3.client("secretsmanager", region_name=REGION)
    secret = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(secret["SecretString"])


def get_parquet_files(s3_client, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def load_parquet_to_table(s3_client, cursor, table, keys):
    for key in keys:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        df = pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas()

        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, df.itertuples(index=False, name=None))


def lambda_handler(event, context):
    """
    Load CDC Chronic and Heart Disease datasets from S3 Parquet into RDS Postgres.

    Steps:
    1. Fetch RDS credentials from Secrets Manager.
    2. For each dataset: create table if missing, truncate, bulk insert from Parquet.

    Args:
        event (dict): Lambda event payload (unused).
        context (LambdaContext): Lambda runtime context.

    Returns:
        dict: Status of the load operation.
    """
    creds = get_rds_credentials()
    s3_client = boto3.client("s3", region_name=REGION)

    conn = psycopg2.connect(
        host=creds["host"],
        port=creds.get("port", 5432),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"]
    )

    try:
        with conn:
            with conn.cursor() as cur:
                for name, s3_prefix in DATASETS.items():
                    table = TABLES[name]
                    schema = TABLE_SCHEMAS[table]

                    cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema});")
                    cur.execute(f"TRUNCATE TABLE {table};")

                    keys = get_parquet_files(s3_client, s3_prefix)
                    load_parquet_to_table(s3_client, cur, table, keys)
    finally:
        conn.close()

    return {"status": "success"}
