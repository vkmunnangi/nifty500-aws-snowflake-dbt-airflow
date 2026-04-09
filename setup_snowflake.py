"""
setup_snowflake.py

One-time setup script for the Lakehouse architecture:
- PIPELINE_CONTROL: per-symbol watermark table (tracks last extract date per symbol)
- BRONZE_STAGE: External Stage pointing to S3
- EXT_DAILY_PRICES: External Table reading Delta Lake from S3
- EXT_NIFTY500_MASTER: External Table reading CSV from S3
- SILVER / GOLD schemas for future dbt transforms

S3 is the single source of truth. Snowflake only computes.
"""

import os
import sys
import snowflake.connector
from dotenv import load_dotenv

# ── Resolve project root ─────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

SF_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "STOCK_MARKET_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
}

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")


# ── SQL statements ───────────────────────────────────────────────────

# Single control table: per-symbol watermark tracking
# New symbol → no row → full 5y backfill
# Existing symbol → last_extract_date is the high watermark → delta from watermark+1
CREATE_PIPELINE_CONTROL = """
CREATE OR REPLACE TABLE PIPELINE_CONTROL (
    symbol              VARCHAR PRIMARY KEY,
    last_extract_date   DATE,
    rows_extracted      INTEGER,
    last_run_id         VARCHAR,
    last_run_timestamp  TIMESTAMP_NTZ,
    last_run_type       VARCHAR,
    last_status         VARCHAR,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

# Parquet file format (Delta uses Parquet under the hood)
CREATE_PARQUET_FORMAT = """
CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = 'PARQUET';
"""

# CSV file format for master data
CREATE_CSV_FORMAT = """
CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
"""

# External stage pointing to S3 bronze layer
CREATE_STAGE_TEMPLATE = """
CREATE OR REPLACE STAGE BRONZE_STAGE
    URL = 's3://{bucket}/bronze/'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_key}'
        AWS_SECRET_KEY = '{aws_secret}'
    );
"""

# External Table for Delta Lake price data
CREATE_EXT_DAILY_PRICES = """
CREATE OR REPLACE EXTERNAL TABLE EXT_DAILY_PRICES (
    date            DATE            AS (VALUE:date::DATE),
    open            FLOAT           AS (VALUE:open::FLOAT),
    high            FLOAT           AS (VALUE:high::FLOAT),
    low             FLOAT           AS (VALUE:low::FLOAT),
    close           FLOAT           AS (VALUE:close::FLOAT),
    volume          BIGINT          AS (VALUE:volume::BIGINT),
    dividends       FLOAT           AS (VALUE:dividends::FLOAT),
    stock_splits    FLOAT           AS (VALUE:stock_splits::FLOAT),
    symbol          VARCHAR         AS (VALUE:symbol::VARCHAR)
)
    LOCATION = @BRONZE_STAGE/daily_prices/
    FILE_FORMAT = PARQUET_FORMAT
    TABLE_FORMAT = DELTA
    REFRESH_ON_CREATE = FALSE
    AUTO_REFRESH = FALSE;
"""

# External Table for master data (CSV)
CREATE_EXT_NIFTY500_MASTER = """
CREATE OR REPLACE EXTERNAL TABLE EXT_NIFTY500_MASTER (
    symbol          VARCHAR         AS (VALUE:c1::VARCHAR),
    company_name    VARCHAR         AS (VALUE:c2::VARCHAR)
)
    LOCATION = @BRONZE_STAGE/master_data/
    FILE_FORMAT = CSV_FORMAT
    AUTO_REFRESH = FALSE;
"""

# Create Silver and Gold schemas for dbt
CREATE_SILVER_SCHEMA = "CREATE SCHEMA IF NOT EXISTS NIFTY500_SILVER;"
CREATE_GOLD_SCHEMA = "CREATE SCHEMA IF NOT EXISTS NIFTY500_GOLD;"


def main():
    print("🔧 Setting up Snowflake objects (Lakehouse architecture)...\n")

    for key, val in SF_CONFIG.items():
        if not val:
            print(f"❌ SNOWFLAKE_{key.upper()} is not set in .env")
            sys.exit(1)

    conn = snowflake.connector.connect(**SF_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Pipeline control table (per-symbol watermarks)
        print("📋 Creating PIPELINE_CONTROL (per-symbol watermark table)...")
        cur.execute(CREATE_PIPELINE_CONTROL)
        print("   ✅ Done")

        # 2. File formats
        print("📄 Creating file formats (PARQUET, CSV)...")
        cur.execute(CREATE_PARQUET_FORMAT)
        cur.execute(CREATE_CSV_FORMAT)
        print("   ✅ Done")

        # 3. External stage
        print(f"☁️  Creating BRONZE_STAGE → s3://{AWS_S3_BUCKET_NAME}/bronze/...")
        stage_sql = CREATE_STAGE_TEMPLATE.format(
            bucket=AWS_S3_BUCKET_NAME,
            aws_key=AWS_ACCESS_KEY_ID,
            aws_secret=AWS_SECRET_ACCESS_KEY,
        )
        cur.execute(stage_sql)
        print("   ✅ Done")

        # 4. External table — Delta Lake prices
        print("📊 Creating EXT_DAILY_PRICES (External Table, Delta format)...")
        print("   → Snowflake reads directly from S3, zero storage cost")
        cur.execute(CREATE_EXT_DAILY_PRICES)
        print("   ✅ Done")

        # 5. External table — Master data CSV
        print("📋 Creating EXT_NIFTY500_MASTER (External Table, CSV)...")
        cur.execute(CREATE_EXT_NIFTY500_MASTER)
        print("   ✅ Done")

        # 6. Silver and Gold schemas
        print("🏗️  Creating SILVER and GOLD schemas...")
        cur.execute(CREATE_SILVER_SCHEMA)
        cur.execute(CREATE_GOLD_SCHEMA)
        print("   ✅ Done")

        # 7. Verify
        print("\n🔍 Verifying created objects...")

        cur.execute("SHOW TABLES IN SCHEMA RAW")
        tables = [row[1] for row in cur.fetchall()]
        print(f"   {'✅' if 'PIPELINE_CONTROL' in tables else '❌'} PIPELINE_CONTROL (per-symbol watermarks)")

        cur.execute("SHOW EXTERNAL TABLES IN SCHEMA RAW")
        ext_tables = [row[1] for row in cur.fetchall()]
        print(f"   {'✅' if 'EXT_DAILY_PRICES' in ext_tables else '❌'} EXT_DAILY_PRICES (external, Delta)")
        print(f"   {'✅' if 'EXT_NIFTY500_MASTER' in ext_tables else '❌'} EXT_NIFTY500_MASTER (external, CSV)")

        cur.execute("SHOW STAGES IN SCHEMA RAW")
        stages = [row[1] for row in cur.fetchall()]
        print(f"   {'✅' if 'BRONZE_STAGE' in stages else '❌'} BRONZE_STAGE")

        cur.execute("SHOW SCHEMAS IN DATABASE STOCK_MARKET_DB")
        schemas = [row[1] for row in cur.fetchall()]
        print(f"   {'✅' if 'SILVER' in schemas else '❌'} SILVER schema")
        print(f"   {'✅' if 'GOLD' in schemas else '❌'} GOLD schema")

        print("\n" + "═" * 55)
        print("🎉 Snowflake setup complete!")
        print("═" * 55)
        print("\nArchitecture:")
        print("  S3 (single source of truth)")
        print("  └── bronze/daily_prices/    ← Delta Lake")
        print("  └── bronze/master_data/     ← CSV")
        print("       ↓")
        print("  Snowflake (compute only)")
        print("  ├── RAW.EXT_DAILY_PRICES    ← External Table (Delta)")
        print("  ├── RAW.EXT_NIFTY500_MASTER ← External Table (CSV)")
        print("  ├── RAW.PIPELINE_CONTROL    ← Per-symbol watermarks")
        print("  ├── SILVER.*                ← dbt transforms")
        print("  └── GOLD.*                  ← dbt aggregations")
        print("═" * 55)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
