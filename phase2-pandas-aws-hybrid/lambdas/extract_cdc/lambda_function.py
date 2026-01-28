import os
import urllib.request
import boto3

# CDC dataset download URLs
urls = [
    "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD"
]

# Local temporary file paths (AWS Lambda writable directory)
local_files = [
    "/tmp/Chronic_Disease.csv",
    "/tmp/Heart_Disease.csv",
    "/tmp/Nutrition.csv"
]

# Target S3 bucket and prefix for raw CDC datasets
s3_bucket = "center-disease-control"
s3_prefix = "raw/"

# Initialize S3 client
s3 = boto3.client("s3")

def lambda_handler(event, context):
    """
    AWS Lambda handler to download CDC public datasets and upload them to S3.

    The function:
    - Downloads multiple CSV datasets from CDC public endpoints
    - Stores them temporarily in the Lambda /tmp directory
    - Uploads each file to an S3 bucket under the raw data prefix
    - Verifies successful upload using head_object

    Args:
        event (dict): Lambda event payload (unused; required by AWS Lambda interface).
        context (LambdaContext): Runtime context provided by AWS Lambda.

    Returns:
        dict: Step Functions–friendly response indicating success or failure.
    """

    # Iterate through each dataset URL and corresponding local file path
    for url, local_file in zip(urls, local_files):
        filename = os.path.basename(local_file)  # Extract filename for S3 key

        # Download CSV file to local /tmp directory
        try:
            print(f"Downloading {filename} from {url} ...")
            urllib.request.urlretrieve(url, local_file)
            print(f"Saved locally as {filename}")
        except Exception as e:
            raise Exception(f"Download failed for {filename}: {e}")

        # Construct S3 object key
        s3_key = f"{s3_prefix}{filename}"

        # Upload file to S3
        try:
            s3.upload_file(local_file, s3_bucket, s3_key)
            print(f"Uploaded {filename} to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            raise Exception(f"S3 upload failed for {filename}: {e}")

        # Verify object exists in S3 after upload
        try:
            s3.head_object(Bucket=s3_bucket, Key=s3_key)
            print(f"Confirmed {s3_key} exists in S3")
        except Exception as e:
            raise Exception(f"Upload verification failed for {s3_key}: {e}")

    # Return success response compatible with AWS Step Functions
    return {
        "data": {
            "success": True,
            "statusCode": 200,
            "body": f"Successfully uploaded files to s3://{s3_bucket}/{s3_prefix}"
        }
    }
