# Cell 1 – Imports & Spark setup
import re
from pyspark.sql import DataFrame, SparkSession
from etl.extract_spark import extract_Center_Disease_data
from pyspark.sql.functions import avg, col, trim, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Cell 2 – Utility functions (generic, reusable)
# Convert CamelCase or spaced column names to snake_case
def camel_to_snake(name: str) -> str:
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

# Drop columns where all values are null
def drop_null_cols(df: DataFrame) -> DataFrame:
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    return df.select(*non_null_cols)

# Rename all columns to snake_case
def rename_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[camel_to_snake(c) for c in df.columns])

# Trim whitespace
def clean_string_columns(df):
    for c, t in df.dtypes:
        if t == 'string':
            df = df.withColumn(c, trim(col(c)))
    return df

# Cell 3 – Cleaning / standardization functions
# Standardize the data_value_unit column
def standardize_data_value_unit(df):
    df_clean = df.replace(
        {
            "per 100,000": "cases per 100,000",
            "  cases per 100,000": "cases per 100,000",
            "cases per 1,000,000": "cases per 1,000,000",
            "cases per 1,000": "cases per 1,000",
            "per 100,000 population": "cases per 100,000"
        },
        subset=["data_value_unit"]
    )
    df_clean = df_clean.withColumn("data_value_unit", F.trim(F.col("data_value_unit")))
    return df_clean

# Drop redundant columns
def drop_columns(df):
    columns = ['data_value_footnote', 'data_value_footnote_symbol']
    cols_to_drop = [c for c in columns if c in df.columns]
    if cols_to_drop:   # only drop if found
        return df.drop(*cols_to_drop)
    return df  # return original df if nothing to drop


# Cleans 'data_value': fills missing with mean for nutrition dataset, else drops NULLs
def clean_data_value(df, name):
    if name.lower() == "clean_df_nutrition":
        mean_val = df.select(avg("data_value")).first()[0]
        df_clean = df.fillna({"data_value": mean_val})
    else:
        df_clean = df.dropna(subset=["data_value"])

    return df_clean

# Cell 4 – Filtering + duplicates
# Keep only rows with location_abbr in US territories
def filter_us_only(df: DataFrame, name="DF") -> DataFrame:
    exclude_locations = ["PR", "GU", "VI", "AS", "MP", "US"]
    filtered_df = df.filter(~col("location_abbr").isin(exclude_locations))

    return filtered_df

# Check and drop duplicate rows
def check_and_drop_duplicates(*dfs_with_names):
    cleaned = []
    for df, name in dfs_with_names:
        dup_count = df.count() - df.dropDuplicates().count()
        cleaned.append(df.dropDuplicates() if dup_count > 0 else df)

    return cleaned

# Cell 6 – apply a row_number window function each dataframe
def apply_window_rank(df: DataFrame, partition_col: str = "location_abbr", order_col: str = "data_value") -> DataFrame:
    """
    Apply row_number() window function to a single DataFrame.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    df_with_rank = df.withColumn("row_num", F.row_number().over(window_spec))
    return df_with_rank

# Cell 5 – Transform pipeline (main function)
# Full ETL transform: drop unnecessary columns, drop nulls, drop duplicates,rename, filter US,
# standardize unit
def transform_Center_Disease_data(df: DataFrame, name="DF") -> DataFrame:
    df_nonull = drop_null_cols(df) #drop nulls
    df_rename = rename_columns(df_nonull) # rename
    df_clean = filter_us_only(df_rename, name=name)# filter
    df_clean = drop_columns(df_clean)  # drop unnecessary columns
    df_clean = clean_string_columns(df_clean)
    df_clean, = check_and_drop_duplicates((df_clean, name)) #df_clean, will failed without ',"

    # standardize value in Chronic/Heart dataframes
    if "data_value_unit" in df_clean.columns:
        df_clean = standardize_data_value_unit(df_clean)
    # fills missing with mean for nutrition dataset, drop nulls value in heart & chronic sets
    if "data_value" in df_clean.columns:
        df_clean = clean_data_value(df_clean, name)

     # Special case: nutrition dataset cleanup
    if name.lower() == "clean_df_nutrition":
        df_clean = df_clean.replace(
            {"Behavioral Risk Factor Surveillance System": "BRESS"}
        )
        # drop da data_value_type (not null, but "value" )
        df_clean = df_clean.drop("data_value_type")
        if "data_value_type" in df_clean.columns:
            print("Failed to drop 'data_value_type' columns in df_nutrition")

    updated_df = apply_window_rank(df_clean)
    updated_df.show()

    return updated_df

