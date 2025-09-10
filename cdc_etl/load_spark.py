import yaml
from pyspark.sql import SparkSession
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
CONFIG_FILE = PROJECT_ROOT / "config.yml"

# Start Spark
spark = SparkSession.builder \
    .appName("CDC_Data") \
    .config("spark.jars", r"C:\PythonProject\CdcDataPipeline\drivers\postgresql-42.7.7.jar") \
    .getOrCreate()

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

pg = config["postgres"]


filepath = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"
df_heart = spark.read.csv(str(filepath), header=True, inferSchema=True)

# Database connection
db_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
db_properties = {
    "user": pg["user"],
    "password": pg["password"],
    "driver": "org.postgresql.Driver"
}

# Write to Postgres table
df_heart.write.jdbc(
    url=db_url,
    table="staging.heart_disease",
    mode="overwrite", properties=db_properties
)
print("Data loaded to Postgres successfully!")

# Read from staging.heart_disease table
df_from_db = spark.read.jdbc(
    url=db_url,
    table="staging.heart_disease",
    properties=db_properties
)
df_from_db.show(5)
