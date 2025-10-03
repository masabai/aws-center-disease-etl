from pyspark.sql import SparkSession
from pathlib import Path
import logging

#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

# Start Spark
spark = SparkSession.builder.appName("CDC_Data").getOrCreate()

# Base path = project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
output_path = str(PROJECT_ROOT / "data" / "processed" / "chronic")

# CSV extraction
def extract_Chronic_Disease_data():
    """
    Extract Chronic Disease datasets from local CSV files into Spark DataFrames.

    This function loads three CDC CSV files:
      1. U.S. Chronic Disease Indicators
      2. Heart Disease Mortality by State/County (2019-2021)
      3. Nutrition, Physical Activity, and Obesity Behavioral Risk Factor Surveillance

    Returns:
        tuple: (df_chronic, df_heart, df_nutrition) as Spark DataFrames
    """
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


def extract_heart_Disease_db():
    pass

df1, df2, df3 =extract_Chronic_Disease_data()
