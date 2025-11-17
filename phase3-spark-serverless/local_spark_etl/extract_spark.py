from pyspark.sql import SparkSession
from pathlib import Path



# Start Spark
spark = SparkSession.builder.appName("CDC_Data").getOrCreate()

# Base path = project root, each parent=each dir, .data/raw-etl-localetl
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
#output_path = str(PROJECT_ROOT / "data" / "processed" / "chronic")

# CSV extraction
def extract_Center_Disease_data():

    filepath1 = DATA_DIR / "Chronic_Disease.csv"
    filepath2 = DATA_DIR / "Heart_Disease.csv"
    filepath3 = DATA_DIR / "Nutrition.csv"

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

df_chronic, df_heart, df_nutrition = extract_Center_Disease_data()
"""
#Debug mode
from pyspark.sql import functions as F
null_counts = df_chronic.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_chronic.columns])
null_counts.show()
"""