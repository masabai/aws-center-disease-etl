from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import logging
import requests
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base path = project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# CSV extraction
def extract_CDC_data_csv():
    """
    Extract Chronic Disease data from local CSV files into Pandas DataFrames.
    """
    filepath = DATA_DIR / "U.S._Chronic_Disease_Indicators.csv"

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    df_chronic = pd.read_csv(filepath)
    return df_chronic

# Postgres extraction using Pandas-to-Spark
def extract_CDC_data_db():
    """
    Extract Chronic Disease data from Postgres into a Spark DataFrame.
    """
    DB_URI = 'postgresql://airflow:airflow@localhost:6543/airflow'
    engine = create_engine(DB_URI)

    # Read data via Pandas- to avoid hadoop file system
    df_pd = pd.read_sql("SELECT * FROM staging.dim_town", con=engine)

    return df_pd

def extract_CDC_data_json():
    # CDC JSON endpoint
    url = 'https://data.cdc.gov/api/views/hksd-2xuw/rows.json?accessType=DOWNLOAD'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        print("Top-level JSON keys and their types:")
        for key, value in data.items():
            print(f"  {key}: type={type(value)}")

        # Extract total count from metadata
        total_count = data['meta']['view']['columns']  # structure is nested, not in ['results']['total']
        print(f"Total columns in dataset: {len(total_count)}")

        # Extract results (actual records are in 'data', not 'results')
        records = data['data']
        print(f"Total rows: {len(records)}")

        # Convert to Pandas
        df_pd = pd.DataFrame(records)
        print("Pandas DataFrame created:")

        return df_pd


df_csv = extract_CDC_data_csv() # as expected 34 rows
#df_db = extract_CDC_data_db() # test db dim_town, seeds cdc data in airflow
#df_json =extract_CDC_data_json() # 44 columns return need to work on this
print(df_csv[['Topic','DataValue',  'DataValueUnit', 'DataValueType']].head(30))