import re
import logging
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def camel_to_snake(name: str) -> str:
    """
    Convert a column name from CamelCase or with spaces/dashes to snake_case.
    Example: 'DataValueFootnote' -> 'data_value_footnote'
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").replace("-", "_").lower()


def drop_null_cols(df_chronic: DataFrame) -> DataFrame:
    """
    Drop columns where all values are null.
    Logs the number of columns before and after dropping.
    """
    logger.info("Number of columns before drop: %d", len(df_chronic.columns))
    non_null_cols = [c for c in df_chronic.columns if df_chronic.filter(df_chronic[c].isNotNull()).count() > 0]
    df_chronic = df_chronic.select(*non_null_cols)
    logger.info("Number of columns after drop: %d", len(df_chronic.columns))
    return df_chronic


def rename_columns(df_chronic: DataFrame) -> DataFrame:
    """
    Rename all columns to snake_case.
    """
    df_clean = df_chronic.toDF(*[camel_to_snake(c) for c in df_chronic.columns])
    return df_clean


def transform_Chronic_Disease_data(df_chronic: DataFrame) -> DataFrame:
    """
    Full transformation pipeline for CDC chronic disease dataset:
    1. Drop all-null columns
    2. Rename columns to snake_case
    3. TODO: handle missing values
    4. TODO: remove duplicates
    """
    clean_df = drop_null_cols(df_chronic)
    clean_df = rename_columns(clean_df)
    return clean_df
