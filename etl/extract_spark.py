from pyspark.sql import SparkSession
from pathlib import Path

# Start Spark
spark = SparkSession.builder.appName("CDC_Data").getOrCreate()

# Base path = project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
output_path = str(PROJECT_ROOT / "data" / "processed" / "chronic")

# CSV extraction
def extract_Center_Disease_data():

    filepath1 = DATA_DIR / "U.S._Chronic_Disease_Indicators.csv"
    filepath2 = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"
    filepath3 = DATA_DIR / "Nutrition__Physical_Activity__and_Obesity_-_Behavioral_Risk_Factor_Surveillance_System.csv"

    for f in [filepath1, filepath2, filepath3]:
        if not f.exists():
            raise FileNotFoundError(f"File not found: {f}")

    #logger.info("Loading CSV files into Spark DataFrames...")
    df_chronic = (spark.read.csv(str(filepath1), header=True, inferSchema=True)).cache()
    df_chronic.count()

    #logger.info(f"Loaded Chronic Disease CSV: {df_chronic.count()} rows")

    df_heart = spark.read.csv(str(filepath2), header=True, inferSchema=True).cache()
    #logger.info(f"Loaded Heart Disease CSV: {df_heart.count()} rows")
    df_heart.count()

    df_nutrition = spark.read.csv(str(filepath3), header=True, inferSchema=True).cache()
    #logger.info(f"Loaded Nutrition CSV: {df_nutrition.count()} rows")
    df_nutrition.count()

    return df_chronic, df_heart, df_nutrition

