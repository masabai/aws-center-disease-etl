# cdc_etl/validate.py

from great_expectations.expectations import ExpectTableColumnCountToEqual, ExpectColumnToExist
import great_expectations as gx
#from cdc_etl import extract, transform  # optional if you want the wrapper to call these

def run_data_validation(dataframe, batch_name="my_batch"):
    """
    Run Great Expectations validation on a single DataFrame.
    Returns True if the checkpoint passes, False otherwise.
    """
    # ------------------- Get context and datasource -------------------
    context = gx.get_context()
    datasource = context.data_sources.add_pandas(name=f"datasource_{batch_name}")
    data_asset = datasource.add_dataframe_asset(name=f"asset_{batch_name}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    # ------------------- Expectation Suite -------------------
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")

    # Expectations
    if "Topic" in dataframe.columns:
        suite.add_expectation(ExpectColumnToExist(column="Topic"))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(dataframe.columns)))

    # Register suite in the Data Context
    suite = context.suites.add(suite=suite)

    # ------------------- Validation Definition -------------------
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )

    # ------------------- Run Validation Definition -------------------
    validation_results = validation_definition.run(
        batch_parameters={"dataframe": dataframe}
    )
    print(f"Validation Definition Success ({batch_name}):", validation_results.success)
    print(validation_results.describe())

    # ------------------- Checkpoint -------------------
    checkpoint = gx.Checkpoint(
        name=f"checkpoint_{batch_name}",
        validation_definitions=[validation_definition]
    )
    checkpoint = context.checkpoints.add(checkpoint=checkpoint)
    checkpoint_results = checkpoint.run(batch_parameters={"dataframe": dataframe})
    print(f"Checkpoint Success ({batch_name}):", checkpoint_results.success)
    print(checkpoint_results.describe())

    return checkpoint_results.success


def validate_pipeline():
    """
    Wrapper to handle all 3 CDC datasets:
    1. Extract
    2. Transform
    3. Validate each DataFrame separately
    Returns a dictionary of validation results.
    """
    # Step 1: Extract
    df1, df2, df3 = extract.extract_Chronic_Disease_data()

    # Step 2: Transform (optional)
    df1 = transform.run(df1)
    df2 = transform.run(df2)
    df3 = transform.run(df3)

    # Step 3: Prepare dictionary of DataFrames
    dfs = {
        "dataset1": df1,
        "dataset2": df2,
        "dataset3": df3
    }

    # Step 4: Validate each DataFrame
    results = {}
    for name, df in dfs.items():
        results[name] = run_data_validation(df, batch_name=name)

    return results
