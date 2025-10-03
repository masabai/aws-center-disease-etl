import re
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from cdc_etl.extract_spark import extract_Chronic_Disease_data

#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)


# Convert a column name from CamelCase or with spaces/dashes to snake_case.
# Example: 'DataValueFootnote' -> 'data_value_footnote'

def camel_to_snake(name: str)-> str:
    # Step 1: your existing logic
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()

    # Step 2: collapse multiple underscores to single
    s3 = re.sub(r'__+', '_', s3)

    # Step 3: strip leading/trailing underscores
    return s3.strip('_')

def drop_null_cols(df: DataFrame) -> DataFrame:
    """
    Drop columns where all values are null.
    Logs the number of columns before and after dropping.
    """
 #   logger.info("Number of columns before drop: %d", len(df_chronic.columns))
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    df_drop_null = df.select(*non_null_cols)
  #  logger.info("Number of columns after drop: %d", len(df_chronic.columns))
    return df_drop_null


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename all columns to snake_case.
    """
    df_clean = df.toDF(*[camel_to_snake(c) for c in df.columns])
    return df_clean

def standarize_data_value_unit(df):
    # Standardize units
    df = df.replace(
        {
            "per 100,000": "cases per 100,000",
            "  cases per 100,000": "cases per 100,000",
            "cases per 1,000,000": "cases per 1,000,000",
            "cases per 1,000": "cases per 1,000",
            "per 100,000 population": "cases per 100,000"  # only 1 value in heart
        },
        subset=["data_value_unit"]
    )

    return df

def transform_Chronic_Disease_data(df: DataFrame) -> DataFrame:
    """
    Full transformation pipeline for CDC chronic disease dataset:
    3. TODO: handle missing values
    4. TODO: remove duplicates
    """
    df_nonull = drop_null_cols(df) # drop null columns
    clean_df = rename_columns(df_nonull) # rename column snake_case

    if "data_value_unit" in clean_df.columns:
      clean_df = standarize_data_value_unit(clean_df)
    return clean_df

# load csv
df_chronic, df_heart, df_nutrition =extract_Chronic_Disease_data()

# transform
clean_df_chronic =transform_Chronic_Disease_data(df_chronic)
clean_df_heart =transform_Chronic_Disease_data(df_heart)
clean_df_nutrition =transform_Chronic_Disease_data(df_nutrition)

#HighConfidenceLimit, "DataValueFootnote"
#distinct_units = standarize_data_value_unit(clean_df_chronic)
#distinct_units.select("data_value_unit").distinct().show()
#distinct_units = clean_df_chronic.select("DataValueFootnote").distinct()
#clean_df_chronic.filter(col('data_value_unit') == 'per 100,000').show()


# Filter rows where data_value_unit is NOT "per 10,000"
#filter_result = clean_df_chronic.filter(col("data_value_unit") == "per 10,000")
filter_result = clean_df_heart.filter(col("data_value_unit") == "per 100,000 population")
#filter_result.show()
print(f"Number of rows in heart: {filter_result.count()}")

