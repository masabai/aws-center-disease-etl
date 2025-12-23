import os
from pathlib import Path
import pandas as pd
import yaml
from sqlalchemy import create_engine

# Determine config file path
if os.name == "nt":
    CONFIG_PATH = Path("C:/PythonProject/CenterDiseaseControl/phase1-pandas-dag/db_config.yml")
else:
    CONFIG_PATH = Path("/opt/airflow/db_config.yml")

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_to_postgres():
    cfg = load_config(CONFIG_PATH)
    pg = cfg["postgres"]
    schema = pg.get("schema", "staging")

    # Build SQLAlchemy connection URI
    pg_uri = (
        f"postgresql+psycopg2://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
    )

    # Create SQLAlchemy engine
    engine = create_engine(pg_uri)
    print("Connected with SQLAlchemy")

    # Base directory
    BASE_DIR = Path(__file__).parent.parent / "data" if os.name == "nt" else Path("/opt/airflow/data")
    PROCESSED_DIR = BASE_DIR / "processed"

    # Map CSV to table names
    TABLE_MAP = {
        "chronic_cleaned.csv": "chronic",
        "heart_cleaned.csv": "heart",
        "nutri_cleaned.csv": "nutri",
    }

    # Loop through CSVs
    for file_name, table in TABLE_MAP.items():
        file_path = PROCESSED_DIR / file_name

        if not file_path.exists():
            print(f"Skipping missing file: {file_path}")
            continue

        print(f"Loading {file_path} → {schema}.{table}")

        df = pd.read_csv(file_path)
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
        print(f"Loaded → {schema}.{table} ({len(df)} rows)")

    print("All CSVs loaded successfully!")

