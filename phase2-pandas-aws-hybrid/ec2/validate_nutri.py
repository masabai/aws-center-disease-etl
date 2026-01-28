import boto3
import json
from datetime import datetime
import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectTableColumnCountToEqual,
    ExpectColumnToExist,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToBeInSet
)

# Initialize S3 client for CDC processed data and GX outputs
s3 = boto3.client("s3")
bucket_name = "center-disease-control"
prefix = "processed/nutri"
gx_prefix = "processed/validation/"

# List all processed CDC nutrition CSV files in S3
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# Iterate through each CSV file and load into a Pandas DataFrame
for file_key in csv_files:
    batch_name = file_key.split('/')[-1].split('.')[0]  # Used for GX batch and suite naming
    s3_path = f"s3://{bucket_name}/{file_key}"
    print(f"Reading {s3_path}")
    df = pd.read_csv(s3_path, storage_options={"anon": False})  # Uses AWS credentials

# Validation Function
def run_data_validation(df, batch_name="my_batch"):
    """
    Run Great Expectations validations on a CDC nutrition dataset.

    Args:
        df (pd.DataFrame): Input dataframe loaded from S3.
        batch_name (str): Logical batch identifier used for GX assets and reports.

    Returns:
        ValidationResult: Great Expectations validation results object.
    """

    # Initialize Great Expectations context
    context = gx.get_context()

    # Create Pandas datasource, asset, and batch definition
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="my_batch")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    # Create and register expectation suite
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    context.suites.add(suite)

    # Table-level expectations
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=50000, max_value=200000))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df.columns) + 1))  # Includes derived column

    # Required column existence checks
    expected_columns = [
        "year_start", "location_abbr", "data_value",
        "low_confidence_limit", "high_confidence_limit",
        "stratification_value"
    ]
    for col in expected_columns:
        suite.add_expectation(ExpectColumnToExist(column=col))

    # Year range validation
    if "year_start" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(
                column="year_start", min_value=2011, max_value=2023
            )
        )

    # Location abbreviation validation
    if "location_abbr" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeInSet(
                column="location_abbr",
                value_set=[
                    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
                    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
                    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
                    "WA", "WV", "WI", "WY",
                    # Territories and special jurisdictions
                    "US", "PR", "GU", "AI", "VI", "MP"
                ]
            )
        )

    # Enforce standard two-letter location codes
    suite.add_expectation(
        ExpectColumnValuesToMatchRegex(column="location_abbr", regex="^[A-Z]{2}$")
    )

    # Data value range validation
    if "data_value" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(
                column="data_value", min_value=0, max_value=86
            )
        )

    # Logical consistency check between confidence limits
    if {"low_confidence_limit", "high_confidence_limit"}.issubset(df.columns):
        df["low_le_high"] = df["low_confidence_limit"] <= df["high_confidence_limit"]
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="low_le_high",
                value_set=[True]
            )
        )

    # Create validation definition
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )

    # Execute validation against the in-memory dataframe
    validation_results = validation_definition.run(
        batch_parameters={"dataframe": df}
    )

    return validation_results

# Save Results to S3
def save_validation_results_s3(validation_results, batch_name):
    """
    Persist Great Expectations validation results to S3 as a timestamped JSON file.

    Args:
        validation_results: GX validation results object.
        batch_name (str): Batch identifier used in output file naming.
    """

    file_name = f"{gx_prefix}gx_{batch_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # Serialize validation results to JSON
    result_dict = validation_results.to_json_dict()
    result_json = json.dumps(result_dict, indent=2)

    # Upload results to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=result_json.encode("utf-8")
    )

    print(f"Validation results uploaded to s3://{bucket_name}/{file_name}")
    print(validation_results.describe())
