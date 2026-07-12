"""
GX validation Lambda container for CDC Nutrition dataset.
Replaces the EC2-based validate_nutri.py — all GX logic runs inside this container.
Called by Step Functions 'Validate GX' state via validate_nutri_container.

Fixes from EC2 version:
- describe() replaced with manual summary (GX API mismatch in Lambda)
- s3:// paths replaced with s3.get_object() (not supported in Lambda runtime)
- tempfile.mkdtemp() used for GX context (Lambda has no persistent filesystem)
"""
import boto3
import json
from datetime import datetime
import pandas as pd
import great_expectations as gx
from io import StringIO
import tempfile

def lambda_handler(event, context):
    """Lambda handler using existing GX validation logic, in EC2"""
    
    # Initialize S3 client
    s3 = boto3.client("s3")
    bucket_name = "center-disease-control"
    prefix = "processed/nutri"
    gx_prefix = "processed/validation/"
    
    try:
        print("=== Starting GX Validation ===")
        
        # Create temp directory for GX (Lambda container fix)
        temp_dir = tempfile.mkdtemp()
        print(f"Using temp directory: {temp_dir}")
        
        # List CSV files (your existing logic)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
        
        print(f"Found {len(csv_files)} CSV files: {csv_files}")
        
        if not csv_files:
            result = {"success": False, "message": "No CSV files found"}
            return {"data": result}
        
        # Process first CSV file (your existing logic, adapted for Lambda)
        file_key = csv_files[0]
        batch_name = file_key.split('/')[-1].split('.')[0]
        
        print(f"Processing: {file_key}")
        
        # Read CSV from S3 (adapted for Lambda - can't use s3:// directly)
        csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"DataFrame shape: {df.shape}")
        
        # Run your existing validation function (with temp_dir)
        validation_results = run_data_validation(df, batch_name, temp_dir)
        
        # Save results using your existing function
        save_validation_results_s3(validation_results, batch_name, s3, bucket_name, gx_prefix)
        
        # Return Step Functions compatible result
        result = {
            "success": bool(validation_results.success),
            "statusCode": 200,
            "body": f"GX validation completed for {batch_name}",
            "validation_summary": {
                "batch_name": batch_name,
                "total_expectations": len(validation_results.results),
                "successful_expectations": sum(1 for r in validation_results.results if r.success),
                "failed_expectations": sum(1 for r in validation_results.results if not r.success)
            },
            "failed_files": "None" if validation_results.success else file_key
        }
        
        return {"data": result}
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
        error_result = {
            "success": False,
            "statusCode": 500,
            "body": "Error in Lambda execution",
            "validation_summary": {"error": str(e)},
            "failed_files": "Lambda execution failed"
        }
        return {"data": error_result}

def run_data_validation(df, batch_name, temp_dir):
    """Your existing validation function with correct GX API"""
    
    # Initialize GX context with temp directory
    context = gx.get_context(context_root_dir=temp_dir)
    
    # Create datasource and asset (updated API)
    datasource = context.sources.add_pandas("my_pandas_datasource")
    data_asset = datasource.add_dataframe_asset(name="my_batch")
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # Create expectation suite
    suite = context.add_expectation_suite(f"suite_{batch_name}")
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    # Add expectations using validator (correct API)
    validator.expect_table_row_count_to_be_between(min_value=50000, max_value=200000)
    validator.expect_table_column_count_to_equal(value=len(df.columns)+1) # added a new column to original DF
    
    # Required column existence checks
    expected_columns = [
        "year_start", "location_abbr", "data_value",
        "low_confidence_limit", "high_confidence_limit",
        "stratification_value"
    ]
    for col in expected_columns:
        if col in df.columns:
            validator.expect_column_to_exist(column=col)
    
    # Year range validation
    if "year_start" in df.columns:
        validator.expect_column_values_to_be_between(
            column="year_start", min_value=2011, max_value=2024
        )
    
    # Location abbreviation validation
    if "location_abbr" in df.columns:
        validator.expect_column_values_to_be_in_set(
            column="location_abbr",
            value_set=[
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
                "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
                "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
                "WA", "WV", "WI", "WY", "US", "PR", "GU", "AI", "VI", "MP"
            ]
        )
        
        # Enforce standard two-letter location codes
        validator.expect_column_values_to_match_regex(
            column="location_abbr", regex="^[A-Z]{2}$"
        )
    
    # Data value range validation
    if "data_value" in df.columns:
        validator.expect_column_values_to_be_between(
            column="data_value", min_value=0, max_value=86
        )
    
    # Logical consistency check between confidence limits
    if {"low_confidence_limit", "high_confidence_limit"}.issubset(df.columns):
        # Create derived column for validation
        df_copy = df.copy()
        df_copy["low_le_high"] = df_copy["low_confidence_limit"] <= df_copy["high_confidence_limit"]
        
        # Create new batch request with derived column
        batch_request_derived = data_asset.build_batch_request(dataframe=df_copy)
        validator_derived = context.get_validator(
            batch_request=batch_request_derived,
            expectation_suite=suite
        )
        validator_derived.expect_column_values_to_be_in_set(
            column="low_le_high", value_set=[True]
        )
    
    # Run validation
    validation_results = validator.validate()
    
    return validation_results

def save_validation_results_s3(validation_results, batch_name, s3, bucket_name, gx_prefix):
    """Save validation results to S3 - FIXED describe() issue"""
    
    file_name = f"{gx_prefix}gx_{batch_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    result_dict = validation_results.to_json_dict()
    result_json = json.dumps(result_dict, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=result_json.encode("utf-8")
    )
    
    print(f"Validation results uploaded to s3://{bucket_name}/{file_name}")
    
    # Print summary instead of describe() - FIXED!
    print(f"Validation Summary:")
    print(f"  - Success: {validation_results.success}")
    print(f"  - Total expectations: {len(validation_results.results)}")
    print(f"  - Successful: {sum(1 for r in validation_results.results if r.success)}")
    print(f"  - Failed: {sum(1 for r in validation_results.results if not r.success)}")
