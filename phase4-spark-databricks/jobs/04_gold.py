# Silver CSV folder paths
silver_csv_paths = {
"chronic": "/Volumes/center_disease_control/cdc/silver/chronic_silver.csv",
"heart": "/Volumes/center_disease_control/cdc/silver/heart_silver.csv",
"nutri": "/Volumes/center_disease_control/cdc/silver/nutri_silver.csv"
}

# Write cleaned DataFrames (from Silver/Transform) to Gold folder
df_chronic_cleaned.write.csv(silver_csv_paths["chronic"], header=True, mode="overwrite")
df_heart_cleaned.write.csv(silver_csv_paths["heart"], header=True, mode="overwrite")
df_nutri_cleaned.write.csv(silver_csv_paths["nutri"], header=True, mode="overwrite")

# Verify columns by reading back as Spark DataFrame
for name, path in silver_csv_paths.items():
    df_check = spark.read.csv(path, header=True, inferSchema=True)
    print(f"{name} columns: {len(df_check.columns)}")
