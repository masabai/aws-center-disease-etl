
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableColumnCountToEqual,
    ExpectColumnToExist,
)

# Import your ETL steps
#from etl.extract_spark import extract_Center_Disease_data
#from etl.transform_spark import transform_Center_Disease_data

# ------------------- Validation Function -------------------
def run_data_validation(dataframe, batch_name="my_batch"):
    """
    Run Great Expectations validation on a Spark DataFrame.
    Returns True if the checkpoint passes, False otherwise.
    """
    # Get context
    context = gx.get_context()

    # Spark datasource
    datasource = context.data_sources.add_or_update_spark(
        name=f"spark_datasource_{batch_name}"
    )
    data_asset = datasource.add_dataframe_asset(name=f"spark_asset_{batch_name}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    # Expectation Suite
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")

    # Expectations
    if "Topic" in dataframe.columns:
        suite.add_expectation(ExpectColumnToExist(column="Topic"))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(dataframe.columns)))

    # Register suite in the Data Context
    suite = context.suites.add(suite=suite)

    # Validation Definition
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )

    # Run Validation
    validation_results = validation_definition.run(
        batch_parameters={"dataframe": dataframe}
    )

    print(f"Validation Definition Success ({batch_name}):", validation_results.success)
    print(validation_results.describe())

    return validation_results.success
