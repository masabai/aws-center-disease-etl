import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, row_number, count
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re

def drop_null_columns(df):
    """
    Drop columns where all values are null.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with non-null columns only.
    """
    non_null_counts = df.agg(*[count(col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    non_null_cols = [c for c, count_val in non_null_counts.items() if count_val > 0]
    return df.select(*non_null_cols)


def camel_to_snake(name):
    """
    Convert CamelCase or spaced names to snake_case.

    Args:
        name (str): Input column name.

    Returns:
        str: Snake_case column name.
    """
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')


def drop_null_cols(df: DataFrame) -> DataFrame:
    """
    Drop columns with all null values.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with non-null columns only.
    """
    non_null_cols = [c for c in df.columns if df.filter(df[c].isNotNull()).count() > 0]
    return df.select(*non_null_cols)


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename DataFrame columns to snake_case.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with renamed columns.
    """
    return df.toDF(*[camel_to_snake(c) for c in df.columns])


def clean_string_columns(df):
    """
    Trim whitespace from all string columns.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with trimmed string columns.
    """
    for c, t in df.dtypes:
        if t == 'string':
            df = df.withColumn(c, trim(col(c)))
    return df


def standardize_data_value_unit(df):
    """
    Standardize 'data_value_unit' column values.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with standardized data_value_unit.
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
    Drop unnecessary footnote columns if present.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame without footnote columns.
    """
    columns = ['data_value_footnote', 'data_value_footnote_symbol']
    cols_to_drop = [c for c in columns if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df


def clean_data_value(df, name):
    """
    Drop rows where 'data_value' is null.

    Args:
        df (DataFrame): Input DataFrame.
        name (str): Dataset name.

    Returns:
        DataFrame: Cleaned DataFrame.
    """
    return df.dropna(subset=["data_value"])


def filter_us_only(df: DataFrame, name="DF") -> DataFrame:
    """
    Keep only US states, filter out territories.

    Args:
        df (DataFrame): Input DataFrame.
        name (str): Dataset name.

    Returns:
        DataFrame: Filtered DataFrame.
    """
    exclude_locations = ["PR", "GU", "VI", "AS", "MP", "US"]
    return df.filter(~col("location_abbr").isin(exclude_locations))


def check_and_drop_duplicates(*dfs_with_names):
    """
    Drop duplicates in one or more DataFrames.

    Args:
        dfs_with_names (tuple): Tuples of (DataFrame, name).

    Returns:
        list: List of cleaned DataFrames.
    """
    cleaned = []
    for df, name in dfs_with_names:
        dup_count = df.count() - df.dropDuplicates().count()
        cleaned.append(df.dropDuplicates() if dup_count > 0 else df)
    return cleaned


def apply_window_rank(df: DataFrame, partition_col: str = "location_abbr", order_col: str = "data_value") -> DataFrame:
    """
    Add a row_number column per partition for ranking.

    Args:
        df (DataFrame): Input DataFrame.
        partition_col (str): Column to partition by.
        order_col (str): Column to order by descending.

    Returns:
        DataFrame: DataFrame with 'row_num' column.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    return df.withColumn("row_num", row_number().over(window_spec))


def transform_Center_Disease_data(df, name="DF"):
    """
    Complete ETL transformation for a CDC dataset.

    Args:
        df (DataFrame): Input DataFrame.
        name (str): Dataset name.

    Returns:
        DataFrame: Transformed DataFrame.
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
        df_clean = apply_window_rank(df_clean)
    return df_clean


@dlt.table(name="cdc_silver_chronic", comment="Silver table for cleaned chronic data")
def silver_chronic():
    df_chronic = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/chronic_delta")
    return transform_Center_Disease_data(df_chronic, "chronic")


@dlt.table(name="cdc_silver_heart", comment="Silver table for cleaned heart data")
def silver_heart():
    df_heart = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/heart_delta")
    return transform_Center_Disease_data(df_heart, "heart")


@dlt.table(name="cdc_silver_nutri", comment="Silver table for cleaned nutrition data")
def silver_nutri():
    df_nutri = spark.read.format("delta").load("/Volumes/center_disease_control/cdc/bronze/nutri_delta")
    return transform_Center_Disease_data(df_nutri, "nutri")
