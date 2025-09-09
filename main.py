"""
Main script to run the CDC Chronic Disease ETL workflow using Spark.
"""
import time
import logging
from pathlib import Path
from cdc_etl.extract_spark import extract_Chronic_Disease_data

from cdc_etl.transform_spark import transform_Chronic_Disease_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def main():
    # Base path = project root
    PROJECT_ROOT = Path(__file__).resolve().parent #add .parent.parent in cdc_etl dir
    DATA_DIR = PROJECT_ROOT / "data" / "raw"

    logger.info("Starting ETL workflow for CDC Chronic Disease dataset.")

    # Extract from csvs
    df_chronic, df_heart, df_nutrition  = extract_Chronic_Disease_data()
    logger.info("Extracted data with %d columns.", len(df_chronic.columns))

    # Extract from postgres Database
    #df_chronic = extract_Chronic_Diease_data_csv(filepath)
    #logger.info("Extracted data with %d columns.", len(df_chronic.columns))

    # Transform
    df_clean = transform_Chronic_Disease_data(df_chronic)
    logger.info("Transformed data now has %d columns.", len(df_clean.columns))

    ############added 9/7/25
    # Cache only here (orchestration level)
    df_clean = df_clean.cache()
    row_count = df_clean.count()   # warm cache
    logger.info("Cached cleaned DataFrame with %d rows.", row_count)
    ############added 9/7/25

    # Show first 3 rows
    logger.info("Displaying the first 3 rows of the cleaned DataFrame:")
    df_clean.show(3, truncate=False)

    ######################added 9/7/25
    # TODO: Load step → save cleaned data
    # df_clean.write.mode("overwrite").parquet(PROJECT_ROOT / "data/processed/cdc_cleaned")


    # Free memory when done
    df_clean.unpersist()
    ###################### added 9/7/25
    logger.info("ETL workflow completed successfully.")

if __name__ == "__main__":
    main()
