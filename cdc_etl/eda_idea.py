from pyspark.sql import functions as F

# -----------------------------
# 1. Quick schema + row count
# -----------------------------
print("Schema:")
df.printSchema()
print(f"Total rows: {df.count():,}")

# -----------------------------
# 2. Missing Data Check
# -----------------------------
missing_summary = (
    df.select([F.count(F.when(F.col(c).isNull() | (F.col(c) == "NULL") | (F.col(c) == "~"), c)).alias(c)
               for c in df.columns])
)
print("Missing values per column:")
missing_summary.show(truncate=False)

# -----------------------------
# 3. Basic stats on numeric column
# -----------------------------
df.select("Data_Value").describe().show()

# More detailed stats
df.select(
    F.mean("Data_Value").alias("mean"),
    F.stddev("Data_Value").alias("stddev"),
    F.min("Data_Value").alias("min"),
    F.max("Data_Value").alias("max")
).show()

# -----------------------------
# 4. Group comparisons
# -----------------------------
# Avg by state
df.groupBy("LocationAbbr").agg(F.mean("Data_Value").alias("avg_value")) \
  .orderBy(F.desc("avg_value")) \
  .show(10)

# Male vs Female
df.groupBy("Stratification1").agg(F.mean("Da
