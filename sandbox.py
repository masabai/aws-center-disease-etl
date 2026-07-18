import boto3
import pandas as pd
import json
from decimal import Decimal

S3_BUCKET = 'center-disease-control'
S3_PREFIX = 'processed/heart/'
DYNAMO_TABLE = 'cdc_heart'
REGION = 'us-west-2'

s3 = boto3.client('s3', region_name=REGION)

response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

for r in response['Contents']:
    print(r['Key'])

