# Databricks notebook source
# MAGIC
# MAGIC %run "./03_validate_dfs"

# COMMAND ----------

# Create a path within your Unity Catalog Volume
volume_path_chronic = "/Volumes/workspace/default/cdc_clean_data/chronic_cleaned.csv"
volume_path_heart = "/Volumes/workspace/default/cdc_clean_data/heart_cleaned.csv"
volume_path_nutri = "/Volumes/workspace/default/cdc_clean_data/nutri_cleaned.csv"

# Write the DataFrame to the specified volume path
df_chronic.write.csv(volume_path_chronic, header=True, mode="overwrite")
df_heart.write.csv(volume_path_heart, header=True, mode="overwrite")
df_nutri.write.csv(volume_path_nutri, header=True, mode="overwrite")

#df_chronic.write.mode("overwrite").parquet(volume_path_chronic)
# Verify data
df_chronic_cleaned = spark.read.csv(volume_path_chronic, header=True, inferSchema=True)
df_chronic.write.mode("overwrite").parquet(volume_path_chronic)
print(len(df_chronic_cleaned.columns))

# Verify data
df_heart_cleaned = spark.read.csv(volume_path_heart, header=True, inferSchema=True)
print(len(df_heart_cleaned.columns))

# Verify data
df_nutri_cleaned = spark.read.csv(volume_path_nutri, header=True, inferSchema=True)
print(len(df_nutri_cleaned.columns))

