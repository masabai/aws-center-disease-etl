import boto3
import time
import json
from datetime import datetime

# Config
INSTANCE_ID = 'i-0ab20fdd90de5418f'
SCRIPT_PATH = '/home/ec2-user/cdc_pandas_etl/scripts/validate_nutri.py'
BUCKET_NAME = 'center-disease-control'
S3_PREFIX = 'processed/validation/'
POLL_INTERVAL = 3  # seconds

# Create Clients
ssm = boto3.client('ssm')
s3 = boto3.client('s3')


def lambda_handler(event, context):
    try:
        # Run GX script on EC2
        response = ssm.send_command(
            InstanceIds=[INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [f"sudo -u ec2-user python3 {SCRIPT_PATH}"]}
        )
        command_id = response['Command']['CommandId']

        # Poll until script completes
        status = "Pending"
        stderr = ""
        exit_code = -1
        while status in ["Pending", "InProgress"]:
            time.sleep(POLL_INTERVAL)
            invocation = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=INSTANCE_ID
            )
            status = invocation["Status"]
            exit_code = invocation.get("ResponseCode", -1)
            stderr = invocation.get("StandardErrorContent", "")

        # Determine success
        success = (status == "Success") and (exit_code == 0)
        error_msg = "" if success else stderr

    except Exception as e:
        command_id = None
        success = False
        error_msg = str(e)

    # Build flat payload like E/T Lambda
    payload = {
        "data": {
            "success": success,
            "statusCode": 200,
            "body": f"GX validation completed on EC2 instance {INSTANCE_ID}"
        }
    }

    # Include failed_files if script failed
    if not success and error_msg:
        payload["data"]["failed_files"] = error_msg

    # Save payload to S3 (optional)
    try:
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{S3_PREFIX}validation_{ts}.json",
            Body=json.dumps(payload, indent=2),
            ContentType='application/json'
        )
        print(f"Validation JSON saved to s3://{BUCKET_NAME}/{S3_PREFIX}validation_{ts}.json")
    except Exception as e:
        print(f"Warning: could not save JSON to S3: {e}")

    # Return flat payload (Step Functions-friendly)
    return payload
