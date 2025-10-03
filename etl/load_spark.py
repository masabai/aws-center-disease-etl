import yaml
from pyspark.sql import SparkSession
from pathlib import Path
from etl.extract_spark import extract_Chronic_Disease_data

# Start Spark
spark = SparkSession.builder.appName("CDC_Data").getOrCreate()


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
CONFIG_FILE = PROJECT_ROOT / "config.yml"

df1, df2, df3 = extract_Chronic_Disease_data()

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

pg = config["postgres"]


#filepath2 = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"
filepath = DATA_DIR / "Nutrition__Physical_Activity__and_Obesity_-_Behavioral_Risk_Factor_Surveillance_System.csv"
# filepath = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"
df_nutri = spark.read.csv(str(filepath), header=True, inferSchema=True)

# Database connection
db_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
db_properties = {
    "user": pg["user"],
    "password": pg["password"],
    "driver": "org.postgresql.Driver"
}

"""
# Write to Postgres table
df_nutri.write.jdbc(
    url=db_url,
    table="staging.nutri",
    mode="overwrite", properties=db_properties
)
print("Data loaded to Postgres successfully!")
"""
# Read from staging.XXXX table
df_from_db = spark.read.jdbc(
    url=db_url,
    table="staging.heart_raw",
    properties=db_properties
)
df_from_db.show(5)

