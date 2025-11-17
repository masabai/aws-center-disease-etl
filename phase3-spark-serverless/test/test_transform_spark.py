from cdc_etl.extract_spark import extract_Chronic_Disease_data
from cdc_etl.transform_spark import transform_Chronic_Disease_data


def test_transform_columns_and_count():
    """
    Test the transformation step for all 3 CSV files.
    Ensures the cleaned DataFrames have rows and columns.
    Dynamically checks for at least one key identifier column.
    """
    df_chronic, df_heart, df_nutrition = extract_Chronic_Disease_data()

    for df in [df_chronic, df_heart, df_nutrition]:
        df_clean = transform_Chronic_Disease_data(df)

        # Basic assertions
        assert df_clean.count() > 0, "Cleaned DataFrame should have rows"
        assert len(df_clean.columns) > 0, "Cleaned DataFrame should have columns"

        # Dynamic key column check
        key_columns = [c for c in df_clean.columns if
                       "year" in c.lower() or "id" in c.lower() or "location" in c.lower()]
        assert len(key_columns) > 0, f"No expected key columns found in {df_clean.columns}"

# Check to see rename work