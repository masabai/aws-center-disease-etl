import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# Function to clean column names

def clean_columns(df):
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


# DLT tables

@dlt.table(
    name="cdc_bronze_chronic",
    comment="Bronze table for chronic data"
)
def get_chronic_data():
    df = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/chronic.csv")
    cleaned_df = clean_columns(df)
    return cleaned_df


@dlt.table(
    name="cdc_bronze_heart",
    comment="Bronze table for heart data"
)
def get_heart_data():
    df = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/heart.csv")
    cleaned_df = clean_columns(df)
    return cleaned_df


@dlt.table(
    name="cdc_bronze_nutri",
    comment="Bronze table for nutri data"
)
def get_nutri_data():
    df = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/nutri.csv")
    cleaned_df = clean_columns(df)
    return cleaned_df

