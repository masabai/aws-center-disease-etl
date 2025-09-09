"""
Sandbox script to demo Spark caching with CDC dataset.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from cdc_etl.extract_spark import extract_Chronic_Disease_data
from cdc_etl.transform_spark import transform_Chronic_Disease_data
import time

# Spark session
spark = SparkSession.builder.appName("CacheDemo").getOrCreate()

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
filepath = DATA_DIR / "U.S._Chronic_Disease_Indicators.csv"

# Extract + Transform
df = extract_Chronic_Disease_data(filepath)
df = transform_Chronic_Disease_data(df)

# Cache the cleaned DataFrame
df = df.cache()
df.count()  # warm the cache

# ---- Demo 1: Aggregation ----
start = time.time()
df.groupBy("location_abbr").avg("data_value").show(3)
print("First aggregation took %.3f sec" % (time.time() - start))

start = time.time()
df.groupBy("location_abbr").avg("data_value").show(3)
print("Second aggregation (cached) took %.3f sec" % (time.time() - start))

# ---- Demo 2: Multiple filters ----
start = time.time()
df.filter(df.location_abbr == "CA").count()
df.filter(df.location_abbr == "NY").count()
df.filter(df.location_abbr == "TX").count()
print("Multiple filters (first run) took %.3f sec" % (time.time() - start))

start = time.time()
df.filter(df.location_abbr == "CA").count()
df.filter(df.location_abbr == "NY").count()
df.filter(df.location_abbr == "TX").count()
print("Multiple filters (cached) took %.3f sec" % (time.time() - start))

# ---- Demo 3: Self-join ----
start = time.time()
df.alias("a").join(df.alias("b"), on="location_abbr").count()
print("First self-join took %.3f sec" % (time.time() - start))

start = time.time()
df.alias("a").join(df.alias("b"), on="location_abbr").count()
print("Second self-join (cached) took %.3f sec" % (time.time() - start))

# Free cache memory
df.unpersist()
spark.stop()
