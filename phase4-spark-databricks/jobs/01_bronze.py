# Read uploaded CSV from volume
df_chronic = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/chronic.csv")
df_heart = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/heart.csv")
df_nutri = spark.read.option("header", "true").csv("/Volumes/center_disease_control/cdc/cdc_raw/nutri.csv")


def clean_columns(df):
    """
    Clean invalid column names by stripping spaces and removing special characters.

    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with cleaned column names.
    """
    for c in df.columns:
        new_c = (
            c.strip()  # remove leading/trailing spaces
            .replace(" ", "_")  # replace spaces with underscore
            .replace("(", "")  # remove opening parentheses
            .replace(")", "")  # remove closing parentheses
            .replace(",", "")  # remove commas
            .replace(";", "")  # remove semicolons
            .replace("=", "")  # remove equal signs
        )
        df = df.withColumnRenamed(c, new_c)  # rename column
    return df


# Apply column cleaning to each dataset
df_chronic = clean_columns(df_chronic)
df_heart = clean_columns(df_heart)
df_nutri = clean_columns(df_nutri)

# Write cleaned DataFrames as Delta tables to volume
df_chronic.write.format("delta").mode("overwrite").save("/Volumes/center_disease_control/cdc/bronze/chronic_delta/")
df_heart.write.format("delta").mode("overwrite").save("/Volumes/center_disease_control/cdc//bronze/heart_delta/")
df_nutri.write.format("delta").mode("overwrite").save("/Volumes/center_disease_control/cdc/bronze/nutri_delta/")

# Preview the first 5 rows of each dataset
df_chronic.show(5)
df_heart.show(5)
df_nutri.show(5)
