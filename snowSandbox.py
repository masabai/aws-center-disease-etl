import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

pg_engine = create_engine(
    "postgresql+psycopg2://airflow:airflow@localhost:6543/airflow"
)
df = pd.read_sql("SELECT * FROM staging.heart_disease", pg_engine)
print(f"Loaded {len(df)} rows from PostgreSQL")

writable_db = "MY_PORTFOLIO_DB"  # set your database name here
schema_name = "CDI_SCHEMA"       # set your schema name here

conn = snowflake.connector.connect(
    user='masabai',
    password='NewStrongPassword123',
    account='WMYOOWI-HZB63745',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN',
    database=writable_db,
    schema=schema_name,
    auto_create_table=True,
)
cs = conn.cursor()

try:
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
except snowflake.connector.errors.ProgrammingError as e:
    raise Exception(f"Cannot create schema in database {writable_db}: {e}")

columns = ", ".join([f"{col} STRING" for col in df.columns])
create_table_sql = f"CREATE TABLE IF NOT EXISTS heart_disease ({columns})"
cs.execute(create_table_sql)
print(f"Table heart_disease created in {writable_db}.{schema_name}")
success, nchunks, nrows, _ = write_pandas(
    conn, df, 'heart_disease', database=writable_db, schema=schema_name
)
print(f"Data insert success: {success}, Rows inserted: {nrows}")

cs.close()
conn.close()

