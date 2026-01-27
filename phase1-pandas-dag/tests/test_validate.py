import great_expectations as gx
from great_expectations.expectations import ExpectTableColumnCountToEqual, ExpectColumnToExist


def create_sample_df():
    """
    Create a small sample pandas DataFrame for testing GE validations.

    Contains:
    - Topic
    - data_value
    - location_abbr
    - row_num
    """
    import pandas as pd
    return pd.DataFrame({
        "Topic": ["Nutrition", "Nutrition"],
        "data_value": [100, 200],
        "location_abbr": ["NY", "CA"],
        "row_num": [1, 2]
    })


def test_validation():
    """
    Validate the sample DataFrame using Great Expectations.

    - Checks table column count
    - Ensures expected columns exist
    - Runs the validation and asserts all expectations pass
    """
    df = create_sample_df()

    # Initialize GE context and datasource
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="my_batch")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="test_batch")

    # Create expectation suite
    suite = gx.ExpectationSuite(name="suite_test_batch")

    # Table-level expectation: column count
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df.columns)))

    # Column-level expectations: required columns exist
    for col in ["Topic", "data_value", "location_abbr", "row_num"]:
        suite.add_expectation(ExpectColumnToExist(column=col))

    # Register suite in context
    suite = context.suites.add(suite=suite)

    # Run validation
    validation_def = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="validation_test_batch"
    )
    result = validation_def.run(batch_parameters={"dataframe": df})

    # Assert all expectations passed
    assert result.success is True
