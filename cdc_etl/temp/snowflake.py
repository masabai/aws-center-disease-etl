
import snowflake.connector

# Connect

conn= snowflake.connector.connect(
        user='snow',
        password='123',
        account='ZCHYFIE-KYB38172',  # e.g., xy12345.us-east-1
        warehouse='COMPUTE_WH',  # your warehouse
        database='MY_PORTFOLIO_DB',  # your DB
        schema='CDI_SCHEMA'  # your schema
    )

cur = conn.cursor()

# Count rows in CDC
# Check current database and schema
cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
print(cur.fetchone())

# List all tables in current schema
cur.execute("SHOW TABLES;")
for row in cur.fetchall():
    print(row)
    cur.close()
conn.close()


