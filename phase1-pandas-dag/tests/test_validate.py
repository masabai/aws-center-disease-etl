import great_expectations as gx
from great_expectations.expectations import ExpectTableColumnCountToEqual, ExpectColumnToExist


def create_sample_df():
    import pandas as pd
    return pd.DataFrame({
        "Topic": ["Nutrition", "Nutrition"],
        "data_value": [100, 200],
        "location_abbr": ["NY", "CA"],
        "row_num": [1, 2]
    })

def test_validation():
    df = create_sample_df()
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="my_batch")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="test_batch")

    suite = gx.ExpectationSuite(name="suite_test_batch")
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df.columns)))
    for col in ["Topic", "data_value", "location_abbr", "row_num"]:
        suite.add_expectation(ExpectColumnToExist(column=col))

    suite = context.suites.add(suite=suite)
    validation_def = gx.ValidationDefinition(data=batch_definition, suite=suite, name="validation_test_batch")
    result = validation_def.run(batch_parameters={"dataframe": df})

    # All expectations passed
    assert result.success is True
