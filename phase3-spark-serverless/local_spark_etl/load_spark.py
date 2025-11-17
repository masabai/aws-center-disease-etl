import yaml
from pathlib import Path

# ------------------- Project Config -------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.yml"

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

pg = config["postgres"]

# Database connection
db_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
db_properties = {
    "user": pg["user"],
    "password": pg["password"],
    "driver": "org.postgresql.Driver"
}

# ------------------- Load to Postgres -------------------
def load_to_pg(df, table_name):
    """
    Write a Spark DataFrame to a Postgres table.
    """
    df.write.jdbc(
        url=db_url,
        table=table_name,
        mode="overwrite",
        properties=db_properties
    )
    print(f"Data loaded to Postgres table '{table_name}' successfully!")

