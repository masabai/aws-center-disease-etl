import boto3
import pandas as pd
import json
from decimal import Decimal

S3_BUCKET = 'center-disease-control'
S3_PREFIX = 'processed/heart/'
DYNAMO_TABLE = 'cdc_heart'
REGION = 'us-west-2'

s3 = boto3.client('s3', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(DYNAMO_TABLE)


def float_to_decimal(obj):
    """Convert floats to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    return obj


def lambda_handler(event, context):
    try:
        # Get all parquet files in S3 prefix
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        parquet_files = [obj['Key'] for obj in response.get('Contents', [])
                         if obj['Key'].endswith('.parquet')]

        if not parquet_files:
            raise Exception(f"No parquet files found at s3://{S3_BUCKET}/{S3_PREFIX}")

        total_written = 0

        for file_key in parquet_files:
            print(f"Reading s3://{S3_BUCKET}/{file_key}")
            local_path = f"/tmp/{file_key.split('/')[-1]}"
            s3.download_file(S3_BUCKET, file_key, local_path)
            df = pd.read_parquet(local_path)

            # Drop rows missing keys
            df = df.dropna(subset=['location_abbr', 'year'])
            df['year'] = df['year'].astype(int)

            # Batch write in chunks of 25 (DynamoDB limit)
            records = df.to_dict('records')
            for i in range(0, len(records), 25):
                chunk = records[i:i + 25]
                with table.batch_writer() as batch:
                    for record in chunk:
                        clean = {k: str(v) if hasattr(v, 'isoformat') else float_to_decimal(v)
                                 for k, v in record.items()
                                 if v is not None and str(v) != 'nan'}
                        batch.put_item(Item=clean)
                total_written += len(chunk)

        return {
            "data": {
                "success": True,
                "statusCode": 200,
                "body": f"Loaded {total_written} records into {DYNAMO_TABLE}",
                "table": DYNAMO_TABLE,
                "files_processed": len(parquet_files)
            }
        }

    except Exception as e:
        return {
            "data": {
                "success": False,
                "statusCode": 500,
                "body": str(e)
            }
        }
