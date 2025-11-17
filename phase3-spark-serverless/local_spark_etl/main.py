# Import ETL steps
from etl.local_etl.extract_spark import extract_Center_Disease_data
from etl.local_etl.transform_spark import transform_Center_Disease_data
from etl.local_etl.validation_gx import run_data_validation
from etl.local_etl.load_spark import load_to_pg


# ------------------- Main Pipeline -------------------
if __name__ == "__main__":

    # CSV extraction
    df_chronic, df_heart, df_nutrition = extract_Center_Disease_data()

    # Transform dataframes
    clean_df_chronic = transform_Center_Disease_data(df_chronic, name="clean_df_chronic")
    clean_df_heart = transform_Center_Disease_data(df_heart, name="clean_df_heart")
    clean_df_nutrition = transform_Center_Disease_data(df_nutrition, name="clean_df_nutrition")

    datasets = {
        "chronic": clean_df_chronic,
        "heart": clean_df_heart,
        "nutrition": clean_df_nutrition
    }

    for batch_name, df in datasets.items():
        table_name = f"clean.{batch_name}"
        load_to_pg(df, table_name)

    print("\nAll clean data loaded to Postgres successfully.")

    # Run GX validation on each
    for batch_name, df in datasets.items():
        print(f"\nRunning GX validation for {batch_name}.validation...")
        validation_results=run_data_validation(df, batch_name=batch_name)
        # save_validation_results_s3(validation_results, batch_name)

    print("\nAll validations complete.")

print("Tesint without s3")


