from pyspark.sql import SparkSession
from pathlib import Path
spark = SparkSession.builder.appName("Test").getOrCreate()


# Base path = project root
PROJECT_ROOT = Path(__file__).resolve().parent #inside pkt add .parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

filepath2 = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"
df_heart = spark.read.csv(str(filepath2), header=True, inferSchema=True)

print(df_heart.columns)
df_heart.select('LocationDesc', 'Topic', 'DataSource').distinct().sort('DataSource').show(5)
