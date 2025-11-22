import os
from pathlib import Path
import pandas as pd
import psycopg2
import yaml

# Determine config path based on environment
if os.name == "nt":  # Windows / local test_spark
    CONFIG_PATH = Path("C:/PythonProject/CenterDiseaseControl/phase1-pandas-dag/db_config.yml")
else:  # Docker / Airflow
    CONFIG_PATH = Path("/opt/airflow/db_config.yml")

def load_config(path):
    """Load YAML DB config"""
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_to_postgres():
    """Load processed CSVs into Postgres, dropping tables every run"""
    # Load DB config
    cfg = load_config(CONFIG_PATH)
    pg = cfg["postgres"]

    # Connect to Postgres
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"]
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("DB connection successful")

    # Directory with processed CSVs
    BASE_DIR = Path(__file__).parent.parent / "data" if os.name == "nt" else Path("/opt/airflow/data")
    PROCESSED_DIR = BASE_DIR / "processed"

    # Tables to load
    TABLE_MAP = {
        "chronic_cleaned.csv": "chronic",
        "heart_cleaned.csv": "heart",
        "nutri_cleaned.csv": "nutri"
    }

    # Create schema if missing
    schema = pg.get("schema", "staging")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Loop through CSVs and load
    for file_name, table in TABLE_MAP.items():
        file_path = PROCESSED_DIR / file_name
        if not file_path.exists():
            print(f"Skipping missing file: {file_path}")
            continue

        print(f"Loading {file_path} → {schema}.{table}")

        # Read CSV header
        df = pd.read_csv(file_path)
        columns_def = ", ".join([f'"{c}" TEXT' for c in df.columns])

        # Drop table if exists (fresh start)
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;')

        # Create table
        cur.execute(f'CREATE TABLE "{schema}"."{table}" ({columns_def});')

        # COPY CSV into Postgres
        copy_sql = f'COPY "{schema}"."{table}" FROM STDIN WITH CSV HEADER'
        with open(file_path, "r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)

        print(f"Loaded → {schema}.{table}")

    cur.close()
    conn.close()
    print("All files loaded successfully")

if __name__ == "__main__":
    load_to_postgres()
