from pathlib import Path
from cdc_etl.extract_spark import extract_Chronic_Disease_data

def test_extract_files_exist():
    """
    Test that all three CDC CSV files are loaded correctly as Spark DataFrames.
    """
    df_chronic, df_heart, df_nutrition = extract_Chronic_Disease_data()

    # Check row counts and column counts
    assert df_chronic.count() == 309215, "Expected 309215 rows for Chronic Disease CSV"
    assert len(df_chronic.columns) == 34, "Expected 34 columns for Chronic Disease CSV"

    assert df_heart.count() == 78792, "Expected 78792 rows for Heart Disease CSV"
    assert len(df_heart.columns) == 21, "Expected 21 columns for Heart Disease CSV"

    assert df_nutrition.count() == 106260, "Expected 106260 rows for Nutrition CSV"
    assert len(df_nutrition.columns) == 33, "Expected 33 columns for Nutrition CSV"

    print("All CSV extraction tests passed!")
