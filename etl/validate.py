import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableColumnCountToEqual,
    ExpectColumnToExist,
)

# ------------------- Validation Function -------------------
def run_data_validation(dataframe, batch_name="my_batch"):
    """
    Run Great Expectations validation on a Spark DataFrame.
    Returns True if the checkpoint passes, False otherwise.
    """
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
    suite.add_expectation(
        gx.expectations.core.ExpectTableRowCountToBeBetween(
            min_value=1,
            max_value=1000000  # adjust based on dataset
        )
    )
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(dataframe.columns)))

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
                max_value=100000
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
            gx.expectations.core.ExpectColumnValuesToBeUnique(column="row_num")
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
    elif "chronic" in batch_name.lower() or "heart" in batch_name.lower():
        if "data_value_unit" in dataframe.columns:
            suite.add_expectation(
                gx.expectations.core.ExpectColumnValuesToBeInSet(
                    column="data_value_unit",
                    value_set=["%", "cases per 100,000", "Number"]
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

    return validation_results.success
