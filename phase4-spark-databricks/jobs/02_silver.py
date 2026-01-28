from pyspark.sql.functions import count, col, trim, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import re

def drop_null_columns(df):
    """
    Drop columns in a DataFrame that contain only null values.
    """
    non_null_counts = df.agg(*[count(col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    non_null_cols = [c for c, count_val in non_null_counts.items() if count_val > 0]
    return df.select(*non_null_cols)

def camel_to_snake(name: str) -> str:
    """
    Convert a column name to snake_case.
    """
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

def drop_null_cols(df: DataFrame) -> DataFrame:
    """
    Select columns that contain at least one non-null value.
    """
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    return df.select(*non_null_cols)

def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename DataFrame columns to snake_case format.
    """
    return df.toDF(*[camel_to_snake(c) for c in df.columns])

def clean_string_columns(df):
    """
    Trim leading and trailing whitespace from all string columns.
    """
    for c, t in df.dtypes:
        if t == 'string':
            df = df.withColumn(c, trim(col(c)))
    return df

def standardize_data_value_unit(df):
    """
    Normalize 'data_value_unit' values to standard naming.
    """
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

def drop_columns(df):
    """
    Remove footnote columns if they exist.
    """
    columns = ['data_value_footnote', 'data_value_footnote_symbol']
    cols_to_drop = [c for c in columns if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df

def clean_data_value(df, name):
    """
    Drop rows where 'data_value' is null.
    """
    return df.dropna(subset=["data_value"])

def filter_us_only(df: DataFrame, name="DF") -> DataFrame:
    """
    Filter out non-US state rows (territories).
    """
    exclude_locations = ["PR", "GU", "VI", "AS", "MP", "US"]
    return df.filter(~col("location_abbr").isin(exclude_locations))

def check_and_drop_duplicates(*dfs_with_names):
    """
    Drop duplicate rows in one or more DataFrames.
    """
    cleaned = []
    for df, name in dfs_with_names:
        dup_count = df.count() - df.dropDuplicates().count()
        cleaned.append(df.dropDuplicates() if dup_count > 0 else df)
    return cleaned

def apply_window_rank(df: DataFrame, partition_col: str = "location_abbr", order_col: str = "data_value") -> DataFrame:
    """
    Add a 'row_num' column ranking by partition and order column.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    return df.withColumn("row_num", row_number().over(window_spec))

def transform_Center_Disease_data(df: DataFrame, name="DF") -> DataFrame:
    """
    Perform full cleaning and transformation of CDC datasets.
    """
    df_nonull = drop_null_cols(df)
    df_rename = rename_columns(df_nonull)
    df_clean = filter_us_only(df_rename, name=name)
    df_clean = drop_columns(df_clean)
    df_clean = clean_string_columns(df_clean)
    df_clean, = check_and_drop_duplicates((df_clean, name))

    if "data_value_unit" in df_clean.columns:
        df_clean = standardize_data_value_unit(df_clean)
    if "data_value" in df_clean.columns:
        df_clean = clean_data_value(df_clean, name)

    updated_df = apply_window_rank(df_clean)
    return updated_df

df_chronic = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/chronic_delta")
df_heart = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/heart_delta")
df_nutri = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/nutri_delta")

df_chronic_cleaned = transform_Center_Disease_data(df_chronic, "chronic")
df_heart_cleaned = transform_Center_Disease_data(df_heart, "heart")
df_nutri_cleaned = transform_Center_Disease_data(df_nutri, "nutri")

silver_csv_paths = {
    "chronic": "/Volumes/center_disease_control/cdc/silver/chronic_silver.csv",
    "heart": "/Volumes/center_disease_control/cdc/silver/heart_silver.csv",
    "nutri": "/Volumes/center_disease_control/cdc/silver/nutri_silver.csv"
}

df_chronic_cleaned.write.csv(silver_csv_paths["chronic"], header=True, mode="overwrite")
df_heart_cleaned.write.csv(silver_csv_paths["heart"], header=True, mode="overwrite")
df_nutri_cleaned.write.csv(silver_csv_paths["nutri"], header=True, mode="overwrite")

df_chronic_cleaned.createOrReplaceTempView("df_chronic_view")
df_heart_cleaned.createOrReplaceTempView("df_heart_view")
df_nutri_cleaned.createOrReplaceTempView("df_nutri_view")

for name, path in silver_csv_paths.items():
    df_check = spark.read.csv(path, header=True, inferSchema=True)
    print(f"{name} columns: {len(df_check.columns)}")
