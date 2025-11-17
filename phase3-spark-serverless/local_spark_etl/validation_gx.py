import boto3
import json
from datetime import datetime
import great_expectations as gx

from great_expectations.expectations import (
    ExpectTableColumnCountToEqual,
    ExpectTableRowCountToBeBetween,
    ExpectColumnToExist,
)

s3 = boto3.client("s3")
bucket_name = "cdc-cloud-demo"
log_prefix = "gx_logs"

# Map expected row counts to transform DataFrames
row_count_ranges = {
    "chronic": (190_000, 210_000),
    "heart": (34_000, 35_000),
    "nutrition": (93_000, 100_000)
}

# Map expected col counts to transform DataFrames
col_count_ranges = {
    "chronic": 23,
    "heart": 20,
    "nutrition": 21
}

# ------------------- Validation Function -------------------
def run_data_validation(dataframe, batch_name="my_batch"):
    # ------------------- Get context -------------------
    context = gx.get_context()

    # ------------------- Spark datasource -------------------
    datasource = context.data_sources.add_or_update_spark(
        name=f"spark_datasource_{batch_name}"
    )
    data_asset = datasource.add_dataframe_asset(name=f"spark_asset_{batch_name}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    # ------------------- Expectation Suite -------------------
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")

    # Table-level expectations
    min_val, max_val = row_count_ranges[batch_name]
    suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=min_val, max_value=max_val)
    )

    # Column-level expectations
    expected_cols = col_count_ranges[batch_name]
    suite.add_expectation(
        ExpectTableColumnCountToEqual(value=expected_cols)
    )

    # Column-level expectations
    required_columns = ["Topic", "data_value", "location_abbr", "row_num"]
    for col in required_columns:
        if col in dataframe.columns:
            suite.add_expectation(ExpectColumnToExist(column=col))
            suite.add_expectation(
                gx.expectations.core.ExpectColumnValuesToNotBeNull(column=col)
            )

    # Value-level expectations
    if "data_value" in dataframe.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(
                column="data_value",
                min_value=0,
                max_value=100000000
            )
        )
    if "location_abbr" in dataframe.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToMatchRegex(
                column="location_abbr",
                regex="^[A-Z]{2}$"
            )
        )
    if "row_num" in dataframe.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToNotBeNull(column="row_num")
        )

    # Dataset-specific expectations
    if "nutrition" in batch_name.lower():
        if "Behavioral Risk Factor Surveillance System" in dataframe.columns:
            suite.add_expectation(
                gx.expectations.core.ExpectColumnValuesToBeInSet(
                    column="Behavioral Risk Factor Surveillance System",
                    value_set=["BRESS"]
                )
            )

    # ------------------- Register suite -------------------
    suite = context.suites.add(suite=suite)

    # ------------------- Validation Definition -------------------
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )

    # ------------------- Run Validation -------------------
    validation_results = validation_definition.run(
        batch_parameters={"dataframe": dataframe}
    )

    print(f"\nValidation Definition Success ({batch_name}):", validation_results.success)
    print(validation_results.describe())

    return validation_results

def save_validation_results_s3(validation_results, batch_name):
    file_name = f"{log_prefix}/gx_{batch_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    result_json = json.dumps(validation_results.to_json_dict(), indent=2)

    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=result_json.encode("utf-8")
    )

    print(f"Validation results uploaded to s3://{bucket_name}/{file_name}")


    """
    sns.publish(
    TopicArn=topic_arn,
    Message='🚨 GX validation failed: missing columns or invalid data values',
    Subject='Data Quality Alert'
)

    """