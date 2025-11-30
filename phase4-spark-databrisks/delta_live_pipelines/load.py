# Paths of raw CSVs (uploaded to your Volume)
raw_csv_paths = {
    "chronic": "/Volumes/workspace/default/cdc_clean_data/chronic_cleaned.csv",
    "heart": "/Volumes/workspace/default/cdc_clean_data/heart_cleaned.csv",
    "nutri": "/Volumes/workspace/default/cdc_clean_data/nutri_cleaned.csv"
}

# Load, transform, and write Bronze Delta
bronze_dfs = {}
for name, path in raw_csv_paths.items():
    df = spark.read.csv(path, header=True, inferSchema=True)
    df_cleaned = transform_cdc(df)

    # Write Bronze
    df_cleaned.write.format("delta").mode("overwrite").save(volume_delta_paths["bronze"][name])

    bronze_dfs[name] = df_cleaned
    print(f"{name} Bronze saved at {volume_delta_paths['bronze'][name]}")
