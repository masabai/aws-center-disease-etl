import boto3
import time
import json
from datetime import datetime

# Config
INSTANCE_ID = 'i-0ab20fdd90de5418f'  # Target EC2 instance for GX script execution
SCRIPT_PATH = '/home/ec2-user/cdc_pandas_etl/scripts/validate_nutri.py'  # Validation script on EC2
BUCKET_NAME = 'center-disease-control'  # S3 bucket to store validation JSON
S3_PREFIX = 'processed/validation/'  # Prefix for validation JSON in S3
POLL_INTERVAL = 3  # Seconds between SSM command status checks

# Create AWS clients
ssm = boto3.client('ssm')  # For running commands on EC2
s3 = boto3.client('s3')    # For saving payload JSON


def lambda_handler(event, context):
    """
    AWS Lambda handler to trigger a Great Expectations validation script
    on a remote EC2 instance via SSM, poll until completion, and save
    results to S3 in Step Functions–friendly JSON format.

    Steps:
    1. Sends SSM command to EC2 to run GX validation script.
    2. Polls for command completion and captures stdout/stderr.
    3. Determines success/failure based on command exit code.
    4. Builds flat payload compatible with E/T Lambda responses.
    5. Optionally uploads payload to S3.

    Args:
        event (dict): Lambda event payload (unused).
        context (LambdaContext): Runtime context provided by AWS Lambda.

    Returns:
        dict: Payload dictionary indicating success, statusCode, body,
              and failed_files (if any).
    """

    try:
        # Run GX validation script on EC2
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

        # Determine success based on SSM status and exit code
        success = (status == "Success") and (exit_code == 0)
        error_msg = "" if success else stderr

    except Exception as e:
        # Catch unexpected errors in SSM call or polling
        command_id = None
        success = False
        error_msg = str(e)

    # Build flat Step Functions–friendly payload
    payload = {
        "data": {
            "success": success,
            "statusCode": 200,
            "body": f"GX validation completed on EC2 instance {INSTANCE_ID}"
        }
    }

    # Include failed files/error info if validation failed
    if not success and error_msg:
        payload["data"]["failed_files"] = error_msg

    # Save payload to S3 (optional, recommended for auditing)
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

    # Return payload for Step Functions or downstream Lambdas
    return payload
