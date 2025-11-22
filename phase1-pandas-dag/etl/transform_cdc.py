import re
import pandas as pd
from pathlib import Path
import os

def camel_to_snake(name: str) -> str:
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = s2.replace(" ", "_").replace("-", "_").lower()
    s3 = re.sub(r'__+', '_', s3)
    return s3.strip('_')

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # (keep your existing cleaning code)
    df.drop_duplicates(inplace=True)
    df.columns = df.columns.str.strip()
    str_cols = df.select_dtypes(include='string').columns
    for col in str_cols:
        if col.lower() == 'location_abbr':
            df[col] = df[col].str.strip().str.upper()
        elif col.lower() == 'location_description':
            df[col] = df[col].str.strip().str.capitalize()
        else:
            df[col] = df[col].str.strip()
    obj_cols = df.select_dtypes(include='object').columns
    for col in obj_cols:
        df[col] = df[col].astype('string').str.strip()
    cols_to_drop = [
        "Topic", "Data_Value_Unit", "Data_Value_Type",
        "Data_Value_Footnote_Symbol", "Data_Value_Footnote",
        "Sex", "Total", "StratificationID1", "StratificationCategoryId1"
    ]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)
    df.columns = [camel_to_snake(col) for col in df.columns]
    numeric_cols = ['year_start','year_end','data_value','data_value_alt','low_confidence_limit','high_confidence_limit','sample_size']
    for col in numeric_cols:
        if col in df.columns:
            if 'year' in col or 'sample' in col:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64').fillna(0)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32').fillna(0)
    rename_map = {
        'age(years)': 'age_range',
        'income': 'income_range',
        'stratification1': 'stratification_value',
        'race/_ethnicity': 'race_ethnicity'
    }
    df.rename(columns={k: v for k,v in rename_map.items() if k in df.columns}, inplace=True)
    if 'stratification_value' in df.columns:
        df['stratification_value'] = df['stratification_value'].astype('string').str.strip()
        df['stratification_value'] = df['stratification_value'].astype('category')
    if 'data_value' not in df.columns or df['data_value'].notnull().sum()==0:
        raise ValueError("Data_Value column missing or entirely null")
    return df

def transform():

    # OS-aware base directory
    if os.name == "nt":  # Windows
        BASE_DIR = Path(__file__).parent.parent / "data"
    else:  # Linux/Docker
        BASE_DIR = Path("/opt/airflow/data")

    RAW_DIR = BASE_DIR / "raw"
    PROCESSED_DIR = BASE_DIR / "processed"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    INPUT_FILES = ['chronic.csv', 'heart.csv', 'nutri.csv']
    OUTPUT_FILES = ['chronic_cleaned.csv', 'heart_cleaned.csv', 'nutri_cleaned.csv']

    for in_file, out_file in zip(INPUT_FILES, OUTPUT_FILES):
        input_path = RAW_DIR / in_file
        output_path = PROCESSED_DIR / out_file

        if not input_path.exists():
            raise FileNotFoundError(f"{input_path} not found — make sure extract task ran first.")

        print(f"Processing {input_path} → {output_path}...")
        df = pd.read_csv(input_path)
        df_clean = clean_dataframe(df)
        df_clean.to_csv(output_path, index=False)
        print(f"Saved cleaned CSV: {output_path}\n")
