import os
import urllib.request
import boto3

# List of dataset download URLs
urls = [
    "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD"
]

# Local temp file paths used in AWS Lambda (/tmp/)
local_files = [
    "/tmp/Chronic_Disease.csv",
    "/tmp/Heart_Disease.csv",
    "/tmp/Nutrition.csv"
]

# S3 target bucket and prefix
s3_bucket = "center-disease-control"
s3_prefix = "raw/"

# Create S3 client
s3 = boto3.client("s3")

def lambda_handler(event, context):
    # Loop through each URL and file path pair
    for url, local_file in zip(urls, local_files):
        # Extract file name only
        filename = os.path.basename(local_file)

        # Download CSV file to /tmp
        try:
            print(f"Downloading {filename} from {url} ...")
            urllib.request.urlretrieve(url, local_file)
            print(f"Saved locally as {filename}")
        except Exception as e:
            raise Exception(f"Download failed for {filename}: {e}")

        # Build S3 object key
        s3_key = f"{s3_prefix}{filename}"

        # Upload file to S3 bucket
        try:
            s3.upload_file(local_file, s3_bucket, s3_key)
            print(f"Uploaded {filename} to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            raise Exception(f"S3 upload failed for {filename}: {e}")

        # Verify that the object exists in S3
        try:
            s3.head_object(Bucket=s3_bucket, Key=s3_key)
            print(f"Confirmed {s3_key} exists in S3")
        except Exception as e:
            raise Exception(f"Upload verification failed for {s3_key}: {e}")

    # Return Step Function–friendly success payload
    return {
        "data": {
            "success": True,
            "statusCode": 200,
            "body": f"Successfully uploaded files to s3://{s3_bucket}/{s3_prefix}"
        }
    }
