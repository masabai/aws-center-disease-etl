import os
from pathlib import Path
import pandas as pd
import yaml
from sqlalchemy import create_engine

CONFIG_PATH = Path("/opt/airflow/db_config.yml")  # Path to DB configuration file

def load_config(path):
    """
    Load YAML configuration file.

    Args:
        path (Path or str): Path to YAML file.

    Returns:
        dict: Parsed configuration dictionary.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_to_postgres():
    """
    Load cleaned CSV files into Postgres using SQLAlchemy.

    Reads CSV files from the processed folder, maps them to table names,
    and writes them to the Postgres database specified in the config.
    """
    cfg = load_config(CONFIG_PATH)
    pg = cfg["postgres"]
    schema = pg.get("schema", "staging")  # Default schema if not provided

    # Build SQLAlchemy connection URI
    pg_uri = (
        f"postgresql+psycopg2://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
    )

    # Create SQLAlchemy engine
    engine = create_engine(pg_uri)
    print("Connected with SQLAlchemy")

    # Determine base directory for CSVs
    BASE_DIR = Path(__file__).parent.parent / "data" if os.name == "nt" else Path("/opt/airflow/data")
    PROCESSED_DIR = BASE_DIR / "processed"

    # Map CSV filenames to Postgres table names
    TABLE_MAP = {
        "chronic_cleaned.csv": "chronic",
        "heart_cleaned.csv": "heart",
        "nutri_cleaned.csv": "nutri",
    }

    # Loop through CSV files and load them to Postgres
    for file_name, table in TABLE_MAP.items():
        file_path = PROCESSED_DIR / file_name

        if not file_path.exists():  # Skip missing files
            print(f"Skipping missing file: {file_path}")
            continue

        print(f"Loading {file_path} → {schema}.{table}")

        df = pd.read_csv(file_path)  # Read CSV
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)  # Load to DB
        print(f"Loaded → {schema}.{table} ({len(df)} rows)")

    print("All CSVs loaded successfully!")
