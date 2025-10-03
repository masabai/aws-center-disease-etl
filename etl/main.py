# Import ETL steps
from etl.extract_spark import extract_Center_Disease_data
from etl.transform_spark import transform_Center_Disease_data
from etl.validate import run_data_validation

# ------------------- Main Pipeline -------------------
if __name__ == "__main__":

    # CSV extraction
    df_chronic, df_heart, df_nutrition = extract_Center_Disease_data()

    # Transform dataframes
    clean_df_chronic = transform_Center_Disease_data(df_chronic, name="clean_df_chronic")
    clean_df_heart = transform_Center_Disease_data(df_heart, name="clean_df_heart")
    clean_df_nutrition = transform_Center_Disease_data(df_nutrition, name="clean_df_nutrition")

    # Map dataset names to transformed DataFrames
    datasets = {
        "chronic_validation": clean_df_chronic,
        "heart_validation": clean_df_heart,
        "nutrition_validation": clean_df_nutrition
    }

    # Run GX validation on each
    for batch_name, df in datasets.items():
        print(f"\nRunning GX validation for {batch_name}...")
        run_data_validation(df, batch_name=batch_name)

    print("\nAll validations complete.")


