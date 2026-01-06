import os

from dotenv import load_dotenv
import snowflake.connector


env_file = os.getenv("ENV_FILE", "")
dotenv_path = (
    env_file
    if env_file
    else os.path.join(os.path.dirname(__file__), "..", ".env")
)
load_dotenv(dotenv_path=dotenv_path)

snowflake_user = (os.getenv("SNOWFLAKE_USER") or "").strip() or None
snowflake_password = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip() or None
snowflake_account = (os.getenv("SNOWFLAKE_ACCOUNT") or "").strip() or None
snowflake_warehouse = (os.getenv("SNOWFLAKE_WAREHOUSE") or "").strip() or None
snowflake_db = (
    (os.getenv("SNOWFLAKE_DB") or "").strip()
    or (os.getenv("SNOWFLAKE_DATABASE") or "").strip()
    or None
)
snowflake_schema = (os.getenv("SNOWFLAKE_SCHEMA") or "").strip() or None
stage_name = (os.getenv("STAGE_NAME") or "bronze_stock_quotes_stage").strip()

if snowflake_schema and "." in snowflake_schema:
    parts = snowflake_schema.split(".")
    if len(parts) >= 2:
        if not snowflake_db:
            snowflake_db = parts[0]
        snowflake_schema = parts[1]

if not all(
    [
        snowflake_user,
        snowflake_password,
        snowflake_account,
        snowflake_warehouse,
        snowflake_db,
        snowflake_schema,
    ]
):
    raise ValueError(
        "SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, "
        "SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DB, and SNOWFLAKE_SCHEMA must be set"
    )

conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_db,
    schema=snowflake_schema,
)

cur = conn.cursor()
try:
    cur.execute(f"USE DATABASE {snowflake_db}")
    cur.execute(f"USE SCHEMA {snowflake_schema}")
    stage_path = f"{snowflake_db}.{snowflake_schema}.{stage_name}"
    cur.execute(f"LIST @{stage_path}")
    rows = cur.fetchall()
    for row in rows:
        print(row)
finally:
    cur.close()
    conn.close()
