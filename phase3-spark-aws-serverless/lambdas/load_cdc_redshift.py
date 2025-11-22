import boto3
import time

# --- Config ---
WORKGROUP_NAME = "default-workgroup"
DATABASE_NAME = "dev"
S3_BUCKET = "center-disease-control"
IAM_ROLE_ARN = "arn:aws:iam::008878757943:role/RedshiftS3LoadRole"

DATASETS = {
    "chronic": "processed/chronic/",
    "heart": "processed/heart/"
}

TABLES = {
    "chronic": "cdc_chronic",
    "heart": "cdc_heart"
}

client = boto3.client('redshift-data', region_name='us-west-2')


def wait_for_statement(stmt_id):
    """Poll Redshift Data API until statement finishes."""
    while True:
        status = client.describe_statement(Id=stmt_id)
        if status["Status"] == "FINISHED":
            return status
        elif status["Status"] in ["FAILED", "ABORTED"]:
            raise Exception(f"Statement {stmt_id} failed: {status.get('Error')}")
        time.sleep(1)


def lambda_handler(event, context):
    for name, s3_path in DATASETS.items():
        table = TABLES[name]

        # Step 0: Truncate table before COPY
        print(f"Truncating table {table}...")
        truncate_sql = f"TRUNCATE TABLE {table};"
        truncate_resp = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=truncate_sql
        )
        wait_for_statement(truncate_resp['Id'])  # WAIT for truncate to complete
        print(f"Table {table} truncated.")

        # Step 1: COPY data from S3
        print(f"Starting COPY for table {table} from s3://{S3_BUCKET}/{s3_path}")
        print(f"Using IAM role: {IAM_ROLE_ARN}")

        copy_sql = f"""
            COPY {table}
            FROM 's3://{S3_BUCKET}/{s3_path}'
            IAM_ROLE '{IAM_ROLE_ARN}'
            FORMAT AS PARQUET;
        """
        copy_resp = client.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DATABASE_NAME,
            Sql=copy_sql
        )
        stmt_id = copy_resp['Id']
        print(f"COPY initiated. Statement ID: {stmt_id}")

        copy_status = wait_for_statement(stmt_id)
        print(f"COPY finished. Status: {copy_status['Status']}")

        if 'Error' in copy_status and copy_status['Error']:
            print(f"Error during COPY for {table}: {copy_status['Error']}")
        else:
            # Optional: get row count
            count_sql = f"SELECT COUNT(*) AS row_count FROM {table};"
            count_resp = client.execute_statement(
                WorkgroupName=WORKGROUP_NAME,
                Database=DATABASE_NAME,
                Sql=count_sql
            )
            count_id = count_resp['Id']
            wait_for_statement(count_id)
            result = client.get_statement_result(Id=count_id)
            rows_loaded = 0
            if 'longValue' in result['Records'][0][0]:
                rows_loaded = result['Records'][0][0]['longValue']
            elif 'stringValue' in result['Records'][0][0]:
                rows_loaded = int(result['Records'][0][0]['stringValue'])
            print(f"Rows currently in {table}: {rows_loaded}")

    return {"status": "success"}

