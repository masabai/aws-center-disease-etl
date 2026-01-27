import pandas as pd
from io import StringIO
import pytest

# Sample CSV string simulating the raw Nutrition CSV for testing
raw_csv = """Topic,Data_Value,Location_Abbr,YearStart,YearEnd,Data_Value_Alt
Nutrition,100,NY,2010,2011,95
Nutrition,200,CA,2010,2011,190
Nutrition,100,NY,2010,2011,95
"""


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw nutrition DataFrame.

    - Removes duplicate rows
    - Strips whitespace from string/object columns
    - Converts numeric columns to float32 and fills missing values with 0
    """
    df = df.copy()
    df.drop_duplicates(inplace=True)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Clean string/object columns
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].astype('string').str.strip()

    # Ensure numeric columns are properly typed
    df['Data_Value'] = pd.to_numeric(df['Data_Value'], errors='coerce').astype('float32').fillna(0)
    df['Data_Value_Alt'] = pd.to_numeric(df['Data_Value_Alt'], errors='coerce').astype('float32').fillna(0)

    return df


def test_transform_load():
    """
    Test the transform function on a small sample CSV.

    - Checks column existence
    - Confirms numeric type conversion
    - Ensures duplicates are removed
    - Validates that numeric columns have non-negative values
    """
    df = pd.read_csv(StringIO(raw_csv))
    df_clean = transform(df)

    # Basic assertions
    assert 'Data_Value' in df_clean.columns
    assert df_clean['Data_Value'].dtype.name == 'float32'
    assert df_clean.shape[0] == 2  # duplicates removed
    assert all(df_clean['Data_Value_Alt'] >= 0)
