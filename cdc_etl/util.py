"""
Helper functions for CDC Chronic Disease ETL pipeline.

Contains:
- inspect_Chronic_Disease_data: performs basic data inspection and
  saves results to Excel (sample rows, summary stats, column types, null counts).
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from pathlib import Path
import pandas as pd
import logging

# Optional: basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Create Spark session
spark = SparkSession.builder.appName("CDC_Data").getOrCreate()

# Base paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
INSPECTION_DIR = PROJECT_ROOT / "data" / "inspection"
filepath = DATA_DIR / "U.S._Chronic_Disease_Indicators.csv"

# Load data
df = spark.read.csv(str(filepath), header=True, inferSchema=True)


def inspect_Chronic_Disease_data(df: DataFrame, inspection_dir: Path = INSPECTION_DIR, max_sample: int = 5) -> None:
    """
    Inspect a Spark DataFrame and save key outputs to Excel.

    Saves:
        - sample_rows: first `max_sample` rows
        - summary: descriptive statistics
        - column_types: column names and types
        - null_counts: number of missing values per column

    Args:
        df (DataFrame): Spark DataFrame to inspect.
        inspection_dir (Path): Directory where inspection files will be saved.
        max_sample (int): Number of rows to include in the sample sheet.
    """
    inspection_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Inspection directory: {inspection_dir}")

    logging.info("Saving combined Excel report...")
    with pd.ExcelWriter(inspection_dir / "inspection_report.xlsx") as writer:
        logging.info(f"Saving {max_sample} sample rows...")
        df.limit(max_sample).toPandas().to_excel(writer, sheet_name="sample_rows", index=False)

        logging.info("Saving summary statistics...")
        df.summary().toPandas().to_excel(writer, sheet_name="summary", index=False)

        logging.info("Saving column types...")
        pd.DataFrame(df.dtypes, columns=["column", "type"]).to_excel(writer, sheet_name="column_types", index=False)

        logging.info("Calculating null counts...")
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        null_counts.toPandas().to_excel(writer, sheet_name="null_counts", index=False)

    logging.info("Inspection complete.")


# Run inspection
inspect_Chronic_Disease_data(df)
