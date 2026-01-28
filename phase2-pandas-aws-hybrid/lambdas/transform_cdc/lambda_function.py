import re
import pandas as pd
import boto3
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')

# S3 source and target locations
BUCKET = "center-disease-control"
KEY = "raw/Nutrition.csv"
OUTPUT_KEY = "processed/nutri/Nutrition_cleaned.csv"

def camel_to_snake(name):
    """
    Convert CamelCase, spaced, or hyphenated column names to snake_case.

    Examples:
        "MyClass" -> "my_class"
        "V1Release" -> "v1_release"
        "Race/Ethnicity" -> "race_ethnicity"
    """

    # Insert underscore before capital letters followed by lowercase letters
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)

    # Handle transitions from lowercase or digits to capital letters
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)

    # Normalize spaces and hyphens, lowercase everything
    s3 = s2.replace(" ", "_").replace("-", "_").lower()

    # Collapse multiple underscores and trim edges
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

def lambda_handler(event, context):
    """
    AWS Lambda handler to clean and normalize CDC Nutrition dataset.

    The transformation performs:
    - Deduplication and whitespace cleanup
    - Column name normalization to snake_case
    - String and categorical standardization
    - Safe numeric casting with null handling
    - Column pruning and schema normalization

    Args:
        event (dict): Lambda event payload (unused).
        context (LambdaContext): Runtime context provided by AWS Lambda.

    Returns:
        dict: Step Functions–compatible success response.
    """

    # Load raw CSV from S3 into a Pandas DataFrame
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    df = pd.read_csv(obj['Body'])

    # Remove duplicate records
    df.drop_duplicates(inplace=True)

    # Trim whitespace from column names
    df.columns = df.columns.str.strip()

    # Clean string-typed columns with domain-specific rules
    str_cols = df.select_dtypes(include='string').columns
    for col in str_cols:
        if col == 'Location_Abbr':  # State abbreviations (e.g., AZ, NY)
            df[col] = df[col].str.strip().str.upper()
        elif col == 'Location_Description':  # Full state names (e.g., New York)
            df[col] = df[col].str.strip().str.capitalize()
        else:
            df[col] = df[col].str.strip()

    # Normalize object columns to Pandas string dtype
    obj_cols = df.select_dtypes(include='object').columns
    for col in obj_cols:
        df[col] = df[col].astype('string').str.strip()

    # Drop redundant or high-missing columns
    cols_to_drop = [
        "Topic", "Data_Value_Unit", "Data_Value_Type",
        "Data_Value_Footnote_Symbol", "Data_Value_Footnote",
        "Sex", "Total", "StratificationID1", "StratificationCategoryId1"
    ]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # Apply snake_case normalization to all column names
    df.columns = [camel_to_snake(col) for col in df.columns]

    # Cast numeric fields with safe coercion and null handling
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
        'race/ethnicity': 'race_ethnicity'
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Standardize stratification values for analytics
    if 'stratification_value' in df.columns:
        df['stratification_value'] = df['stratification_value'].astype('string').str.strip()
        df['stratification_value'] = df['stratification_value'].astype('category')

    # Enforce presence of critical metric column
    if 'data_value' not in df.columns or df['data_value'].notnull().sum() == 0:
        raise ValueError("Data_Value column missing or entirely null")

    # Write cleaned dataset back to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=csv_buffer.getvalue())

    # Return Step Functions–compatible response
    return {
        "data": {
            "success": True,
            "statusCode": 200,
            "body": f"Transformation complete. Output saved to {OUTPUT_KEY}"
        }
    }
