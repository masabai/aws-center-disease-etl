
import re
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, trim
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pathlib import Path

def camel_to_snake(name):
    """
    Convert CamelCase, spaced, or hyphenated column names to snake_case.
    """
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

def drop_null_cols(df):
    """
    Drop columns that are entirely null.
    """
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    return df.select(*non_null_cols)


def rename_columns(df):
    """
    Rename all columns to snake_case.
    """
    return df.toDF(*[camel_to_snake(c) for c in df.columns])


def clean_string_columns(df):
    """
    Trim whitespace from all string columns.
    """
    for c, t in df.dtypes:
        if t == 'string':
            df = df.withColumn(c, trim(col(c)))
    return df


def standardize_data_value_unit(df):
    """
    Normalize values in the 'data_value_unit' column.
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
    Drop redundant or high-missing columns.
    """
    columns = ['data_value_footnote', 'data_value_footnote_symbol']
    cols_to_drop = [c for c in columns if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df


def clean_data_value(df, name):
    """
    Drop rows where 'data_value' is null.
    """
    return df.dropna(subset=["data_value"])


def filter_us_only(df, name="DF"):
    """
    Filter out non-US locations or territories.
    """
    exclude_locations = ["PR", "GU", "VI", "AS", "MP", "US"]
    return df.filter(~col("location_abbr").isin(exclude_locations))


def check_and_drop_duplicates(*dfs_with_names):
    """
    Drop duplicate rows for each DataFrame.
    """
    cleaned = []
    for df, name in dfs_with_names:
        dup_count = df.count() - df.dropDuplicates().count()
        cleaned.append(df.dropDuplicates() if dup_count > 0 else df)
    return cleaned


def apply_window_rank(df, partition_col="location_abbr", order_col="data_value"):
    """
    Add a 'row_num' column using window partitioning to rank data values per location.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    return df.withColumn("row_num", F.row_number().over(window_spec))


def transform_Center_Disease_data(df, name="DF"):
    """
    Full transformation pipeline for CDC Chronic/Heart disease data.
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


def main():
    """
    Spark-based ETL pipeline for CDC Chronic and Heart Disease datasets.
    """
    spark = SparkSession.builder.appName("CDC_Serverless_ETL").getOrCreate()

    s3_bucket = "center-disease-control"
    raw_prefix = "raw/"
    staging_prefix = "processed/"

    datasets = {
        "chronic": "Chronic_Disease.csv",
        "heart": "Heart_Disease.csv"
    }

    for name, filename in datasets.items():
        # Load raw CSV from S3
        path = f"s3://{s3_bucket}/{raw_prefix}{filename}"
        df = spark.read.csv(path, header=True, inferSchema=True)

        # Transform dataset
        df_clean = transform_Center_Disease_data(df, name=name)

        # Write cleaned dataset to S3 as Parquet
        out_path = f"s3://{s3_bucket}/{staging_prefix}{name}/"
        df_clean.write.mode("overwrite").parquet(out_path)
        # Note: single parquet file not used to avoid Spark coalesce overhead

        print(f"{name} cleaned and saved to {out_path}")
