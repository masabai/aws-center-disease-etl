import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def clean_columns(df):
    """
    Clean DataFrame column names by stripping whitespace and replacing
    spaces and special characters with underscores or removing them.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame to clean.

    Returns:
        pyspark.sql.DataFrame: DataFrame with cleaned column names.
    """
    for c in df.columns:
        new_c = (
            c.strip()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace(";", "")
            .replace("=", "")
        )
        df = df.withColumnRenamed(c, new_c)
    return df


@dlt.table(
    name="cdc_bronze_chronic",
    comment="Bronze table for chronic disease data"
)
def get_chronic_data():
    """
    Load and clean CDC chronic disease CSV as a DLT bronze table.

    Returns:
        pyspark.sql.DataFrame: Cleaned chronic disease DataFrame.
    """
    df = spark.read.option("header", "true").csv(
        "/Volumes/center_disease_control/cdc/cdc_raw/chronic.csv"
    )
    cleaned_df = clean_columns(df)
    return cleaned_df


@dlt.table(
    name="cdc_bronze_heart",
    comment="Bronze table for heart disease data"
)
def get_heart_data():
    """
    Load and clean CDC heart disease CSV as a DLT bronze table.

    Returns:
        pyspark.sql.DataFrame: Cleaned heart disease DataFrame.
    """
    df = spark.read.option("header", "true").csv(
        "/Volumes/center_disease_control/cdc/cdc_raw/heart.csv"
    )
    cleaned_df = clean_columns(df)
    return cleaned_df


@dlt.table(
    name="cdc_bronze_nutri",
    comment="Bronze table for nutrition data"
)
def get_nutri_data():
    """
    Load and clean CDC nutrition CSV as a DLT bronze table.

    Returns:
        pyspark.sql.DataFrame: Cleaned nutrition DataFrame.
    """
    df = spark.read.option("header", "true").csv(
        "/Volumes/center_disease_control/cdc/cdc_raw/nutri.csv"
    )
    cleaned_df = clean_columns(df)
    return cleaned_df
