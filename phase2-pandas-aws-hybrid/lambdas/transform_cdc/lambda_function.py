import re
import pandas as pd
import boto3
from io import StringIO

s3 = boto3.client('s3')

# S3 target bucket and prefix
BUCKET = "center-disease-control"
KEY = "raw/Nutrition.csv"
OUTPUT_KEY = "processed/nutri/Nutrition_cleaned.csv"

# Convert CamelCase or spaced names to snake_case
def camel_to_snake(name: str) -> str:
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

def lambda_handler(event, context):
    # Load CSV from S3
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    df = pd.read_csv(obj['Body'])

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Trim whitespace in column names
    df.columns = df.columns.str.strip()

    # Clean string columns
    str_cols = df.select_dtypes(include='string').columns
    for col in str_cols:
        if col == 'Location_Abbr':
            df[col] = df[col].str.strip().str.upper()
        elif col == 'Location_Description':
            df[col] = df[col].str.strip().str.capitalize()
        else:
            df[col] = df[col].str.strip()

    # Normalize object columns
    obj_cols = df.select_dtypes(include='object').columns
    for col in obj_cols:
        df[col] = df[col].astype('string').str.strip()

    # Drop redundant / high-missing columns
    cols_to_drop = [
        "Topic", "Data_Value_Unit", "Data_Value_Type",
        "Data_Value_Footnote_Symbol", "Data_Value_Footnote",
        "Sex", "Total", "StratificationID1", "StratificationCategoryId1"
    ]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # Apply camel_to_snake to all columns
    df.columns = [camel_to_snake(col) for col in df.columns]

    # Cast numeric fields with safe conversions
    df['year_start'] = df['year_start'].astype('Int64')
    df['year_end'] = df['year_end'].astype('Int64')
    df['data_value'] = pd.to_numeric(df['data_value'], errors='coerce').astype('float32').fillna(0)
    df['data_value_alt'] = pd.to_numeric(df['data_value_alt'], errors='coerce').astype('float32').fillna(0)
    df['low_confidence_limit'] = pd.to_numeric(df['low_confidence_limit'], errors='coerce').astype('float32').fillna(0)
    df['high_confidence_limit'] = pd.to_numeric(df['high_confidence_limit'], errors='coerce').astype('float32').fillna(0)
    df['sample_size'] = pd.to_numeric(df['sample_size'], errors='coerce').astype('Int64').fillna(0)

    # Rename inconsistent categorical columns
    rename_map = {
        'age(years)': 'age_range',
        'income': 'income_range',
        'stratification1': 'stratification_value',
        'race/_ethnicity': 'race_ethnicity'
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Standardize stratification_value
    if 'stratification_value' in df.columns:
        df['stratification_value'] = df['stratification_value'].astype('string').str.strip()
        df['stratification_value'] = df['stratification_value'].astype('category')

    # Critical validation
    if 'data_value' not in df.columns or df['data_value'].notnull().sum() == 0:
        raise ValueError("Data_Value column missing or entirely null")

    # Write cleaned CSV back to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=csv_buffer.getvalue())

    # Return Step Functions-compatible payload
    return {
      "data": {
        "success": True,
        "statusCode": 200,
        "body": f"Transformation complete. Output saved to {OUTPUT_KEY}"
      }
    }
