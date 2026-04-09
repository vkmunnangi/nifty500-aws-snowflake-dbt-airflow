"""
yfinance_to_s3.py

Production-grade extractor with per-symbol watermark tracking:

1. Reads PIPELINE_CONTROL → get per-symbol high watermarks
2. Splits symbols into:
   - NEW symbols (not in control table) → full 5y backfill
   - DELTA symbols (watermark < today) → incremental from watermark+1
   - UP-TO-DATE symbols (watermark >= today) → skip
3. Downloads data via yfinance (thread pool + retries)
4. Writes Delta Lake format to S3
5. Updates per-symbol watermarks in PIPELINE_CONTROL

S3 is the single source of truth. Snowflake reads via External Tables.
"""

import os
import sys
import time
import uuid
import json
import logging
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
import pyarrow as pa
import yfinance as yf
import snowflake.connector
from deltalake import write_deltalake, DeltaTable
from dotenv import load_dotenv

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("peewee").setLevel(logging.ERROR)

# ── Resolve project root ─────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ── AWS config ───────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

S3_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "timeout": "600s",           # default 180s too short for India → eu-north-1
    "connect_timeout": "30s",
}

# ── Snowflake config ─────────────────────────────────────────────────
SF_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "STOCK_MARKET_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
}

# ── Paths & constants ────────────────────────────────────────────────
MASTER_CSV_PATH = os.path.join(PROJECT_ROOT, "nifty500_master.csv")
S3_MASTER_KEY = "bronze/master_data/nifty500_master.csv"
S3_DELTA_TABLE_URI = f"s3://{AWS_S3_BUCKET_NAME}/bronze/daily_prices"

FULL_LOAD_PERIOD = "5y"
MAX_WORKERS = 5
BATCH_SIZE = 50
BATCH_DELAY_SECS = 3
MAX_RETRIES = 3
RETRY_BACKOFF_SECS = 5


# ── Environment validation ──────────────────────────────────────────
def validate_env() -> None:
    placeholders = ["your_aws", "your_globally", "your_account"]
    for name, val in [
        ("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID),
        ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
        ("AWS_S3_BUCKET_NAME", AWS_S3_BUCKET_NAME),
    ]:
        if not val or any(p in val.lower() for p in placeholders):
            log.error(f"❌ {name} is not set or has placeholder value")
            sys.exit(1)


# ── Snowflake helpers ────────────────────────────────────────────────
def get_snowflake_connection():
    return snowflake.connector.connect(**SF_CONFIG)


def get_symbol_watermarks() -> dict[str, date]:
    """
    Read PIPELINE_CONTROL and return a dict of {symbol: last_extract_date}.
    Symbols not in the dict are NEW and need a full backfill.
    """
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, last_extract_date
            FROM PIPELINE_CONTROL
            WHERE last_status = 'SUCCESS'
        """)
        watermarks = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()
        log.info(f"📅 Loaded watermarks for {len(watermarks)} symbols from control table")
        return watermarks
    except Exception as e:
        log.warning(f"⚠️  Could not read control table: {e}")
        return {}


def update_symbol_watermarks(
    run_id: str,
    symbol_results: dict[str, dict],
) -> None:
    """
    MERGE per-symbol results into PIPELINE_CONTROL.
    Uses a single BULK query instead of 500 individual queries to save 5+ mins.
    """
    if not symbol_results:
        return

    try:
        # Build the values list for bulk insert
        values_lists = []
        for symbol, info in symbol_results.items():
            # Escape strings to prevent SQL errors
            sym_clean = symbol.replace("'", "''")
            status_clean = info["status"].replace("'", "''")
            type_clean = info["type"].replace("'", "''")
            
            val = (
                f"('{sym_clean}', '{info['last_date'].isoformat()}', "
                f"{info['rows']}, '{run_id}', '{type_clean}', '{status_clean}')"
            )
            values_lists.append(val)
        
        values_str = ",\n".join(values_lists)

        conn = get_snowflake_connection()
        cur = conn.cursor()

        bulk_merge_sql = f"""
        MERGE INTO PIPELINE_CONTROL AS target
        USING (
            SELECT 
                column1 AS symbol, 
                column2::DATE AS last_extract_date,
                column3::INTEGER AS rows_extracted,
                column4 AS last_run_id,
                column5 AS last_run_type,
                column6 AS last_status
            FROM VALUES 
            {values_str}
        ) AS source
        ON target.symbol = source.symbol
        WHEN MATCHED THEN UPDATE SET
            last_extract_date = source.last_extract_date,
            rows_extracted    = source.rows_extracted,
            last_run_id       = source.last_run_id,
            last_run_timestamp = CURRENT_TIMESTAMP(),
            last_run_type     = source.last_run_type,
            last_status       = source.last_status,
            updated_at        = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            symbol, last_extract_date, rows_extracted,
            last_run_id, last_run_timestamp, last_run_type, last_status
        ) VALUES (
            source.symbol, source.last_extract_date, source.rows_extracted,
            source.last_run_id, CURRENT_TIMESTAMP(), source.last_run_type, source.last_status
        )
        """
        
        cur.execute(bulk_merge_sql)
        cur.close()
        conn.close()
        log.info(f"📝 Bulk updated watermarks for {len(symbol_results)} symbols in 1 query")
    except Exception as e:
        log.error(f"❌ Failed to bulk update control table: {e}")


# ── S3 helper (master CSV only) ─────────────────────────────────────
def upload_master_to_s3() -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3.upload_file(MASTER_CSV_PATH, AWS_S3_BUCKET_NAME, S3_MASTER_KEY)
    log.info(f"☁️  Uploaded master list → s3://{AWS_S3_BUCKET_NAME}/{S3_MASTER_KEY}")


# ── yfinance download ───────────────────────────────────────────────
def fetch_symbol_history(
    symbol: str,
    period: str | None = None,
    start_date: date | None = None,
) -> pd.DataFrame:
    """Download OHLCV data with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol)

            if start_date:
                df = ticker.history(start=start_date.isoformat())
            else:
                df = ticker.history(period=period or FULL_LOAD_PERIOD)

            if df.empty:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECS * attempt)
                    continue
                return pd.DataFrame()

            df["Symbol"] = symbol
            df.reset_index(inplace=True)

            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)

            return df

        except Exception as exc:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECS * attempt)
            else:
                log.error(f"❌ Giving up on {symbol}: {exc}")
                return pd.DataFrame()

    return pd.DataFrame()


def download_symbols(
    symbols: list[str],
    period: str | None = None,
    start_date: date | None = None,
    label: str = "",
) -> tuple[list[pd.DataFrame], list[str]]:
    """Download data in batches. Returns (frames, failed_symbols)."""
    all_frames: list[pd.DataFrame] = []
    failed: list[str] = []
    total = len(symbols)
    done = 0
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_start in range(0, total, BATCH_SIZE):
        batch = symbols[batch_start: batch_start + BATCH_SIZE]
        batch_num = (batch_start // BATCH_SIZE) + 1
        log.info(f"━━ {label} Batch {batch_num}/{total_batches} ━━")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_sym = {
                executor.submit(
                    fetch_symbol_history, sym,
                    period=period, start_date=start_date,
                ): sym
                for sym in batch
            }

            for future in as_completed(future_to_sym):
                symbol = future_to_sym[future]
                done += 1
                try:
                    df = future.result()
                    if not df.empty:
                        all_frames.append(df)
                        log.info(f"✅ [{done:3d}/{total}] {symbol:20s} → {len(df):>5} rows")
                    else:
                        failed.append(symbol)
                        log.warning(f"⚠️  [{done:3d}/{total}] {symbol:20s} → no data")
                except Exception as exc:
                    failed.append(symbol)
                    log.error(f"❌ [{done:3d}/{total}] {symbol:20s} → {exc}")

        if batch_start + BATCH_SIZE < total:
            time.sleep(BATCH_DELAY_SECS)

    return all_frames, failed


# ── Delta Lake writer ────────────────────────────────────────────────
def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names, types, and select only needed columns."""
    df = df.rename(columns={
        "Date": "date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume", "Dividends": "dividends",
        "Stock Splits": "stock_splits", "Symbol": "symbol",
    })

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["volume"] = df["volume"].astype("int64")

    keep_cols = ["date", "open", "high", "low", "close", "volume",
                 "dividends", "stock_splits", "symbol"]
    return df[[c for c in keep_cols if c in df.columns]]


def delta_table_exists() -> bool:
    """Check if the Delta table already exists on S3."""
    try:
        DeltaTable(S3_DELTA_TABLE_URI, storage_options=S3_STORAGE_OPTIONS)
        return True
    except Exception:
        return False


def write_to_delta(df: pd.DataFrame) -> None:
    """
    Write to Delta Lake using MERGE (upsert) for idempotent writes.

    - First run (table doesn't exist): creates in chunks to avoid S3 timeouts
    - Subsequent runs: MERGE on (symbol, date) — no duplicates ever
    """
    df = prepare_dataframe(df)
    total_rows = len(df)
    log.info(f"📝 Writing {total_rows:,} rows to Delta Lake...")
    log.info(f"   📍 {S3_DELTA_TABLE_URI}")

    if not delta_table_exists():
        # First time — create the table (single file, no chunking)
        log.info("   🆕 Table doesn't exist → creating with initial data")
        write_deltalake(
            S3_DELTA_TABLE_URI,
            df,
            mode="overwrite",
            storage_options=S3_STORAGE_OPTIONS,
        )
    else:
        # Table exists — MERGE (upsert) on symbol + date
        log.info("   🔀 Table exists → MERGE (upsert) on symbol + date")
        dt = DeltaTable(S3_DELTA_TABLE_URI, storage_options=S3_STORAGE_OPTIONS)
        pa_table = pa.Table.from_pandas(df, preserve_index=False)

        (
            dt.merge(
                source=pa_table,
                predicate="target.symbol = source.symbol AND target.date = source.date",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    # Log table stats
    dt = DeltaTable(S3_DELTA_TABLE_URI, storage_options=S3_STORAGE_OPTIONS)
    log.info(f"   ✅ Delta version: {dt.version()}")
    log.info(f"   📂 Files: {len(dt.file_uris())}")

    # Vacuum orphan files (from any crashed previous runs)
    log.info("   🧹 Vacuuming orphan files...")
    dt.vacuum(retention_hours=0, enforce_retention_duration=False)
    log.info("   ✅ Vacuum complete")


# ── Main pipeline ────────────────────────────────────────────────────
def main() -> None:
    start_time = time.time()
    run_id = str(uuid.uuid4())[:8]
    today = date.today()

    # 0. Validate
    validate_env()

    # 1. Read master list
    if not os.path.exists(MASTER_CSV_PATH):
        raise FileNotFoundError(
            f"{MASTER_CSV_PATH} not found. Run create_master_list.py first."
        )
    master_df = pd.read_csv(MASTER_CSV_PATH)
    all_symbols = master_df["Symbol"].tolist()
    log.info(f"📋 Loaded {len(all_symbols)} symbols from master list")

    # 2. Upload master CSV to S3
    log.info("🔼 Uploading master list to S3...")
    upload_master_to_s3()

    # 3. Get per-symbol watermarks from control table
    watermarks = get_symbol_watermarks()

    # 4. Split symbols into groups
    new_symbols = [s for s in all_symbols if s not in watermarks]
    delta_symbols = [
        s for s in all_symbols
        if s in watermarks and watermarks[s] < today
    ]
    up_to_date = [
        s for s in all_symbols
        if s in watermarks and watermarks[s] >= today
    ]

    log.info(f"\n📊 Symbol classification:")
    log.info(f"   🆕 New (full 5y backfill):  {len(new_symbols)}")
    log.info(f"   📈 Delta (incremental):     {len(delta_symbols)}")
    log.info(f"   ✅ Up-to-date (skip):       {len(up_to_date)}")

    all_frames: list[pd.DataFrame] = []
    symbol_results: dict[str, dict] = {}

    # 5. Download NEW symbols (full 5y backfill)
    if new_symbols:
        log.info(f"\n🆕 FULL LOAD: Downloading {FULL_LOAD_PERIOD} for "
                 f"{len(new_symbols)} new symbols...")
        frames, failed = download_symbols(
            new_symbols, period=FULL_LOAD_PERIOD, label="[NEW]",
        )
        all_frames.extend(frames)

        # Track results per symbol
        for df in frames:
            sym = df["Symbol"].iloc[0]
            max_date = pd.to_datetime(df["Date"]).max()
            symbol_results[sym] = {
                "last_date": max_date.date() if hasattr(max_date, "date") else max_date,
                "rows": len(df),
                "type": "FULL",
                "status": "SUCCESS",
            }
        for sym in failed:
            symbol_results[sym] = {
                "last_date": today,
                "rows": 0,
                "type": "FULL",
                "status": "FAILED",
            }

    # 6. Download DELTA symbols (incremental)
    if delta_symbols:
        # Group by watermark date to minimize different start_dates
        # For simplicity, use the MIN watermark as start for all delta symbols
        # (a few extra rows is fine — Delta Lake handles dedup at query time)
        min_watermark = min(watermarks[s] for s in delta_symbols)
        start_date = min_watermark + timedelta(days=1)

        log.info(f"\n📈 DELTA LOAD: Downloading from {start_date} for "
                 f"{len(delta_symbols)} symbols...")
        frames, failed = download_symbols(
            delta_symbols, start_date=start_date, label="[DELTA]",
        )
        all_frames.extend(frames)

        for df in frames:
            sym = df["Symbol"].iloc[0]
            max_date = pd.to_datetime(df["Date"]).max()
            symbol_results[sym] = {
                "last_date": max_date.date() if hasattr(max_date, "date") else max_date,
                "rows": len(df),
                "type": "DELTA",
                "status": "SUCCESS",
            }
        for sym in failed:
            symbol_results[sym] = {
                "last_date": watermarks.get(sym, today),
                "rows": 0,
                "type": "DELTA",
                "status": "FAILED",
            }

    # 7. Retry failed symbols
    failed_all = [s for s, r in symbol_results.items() if r["status"] == "FAILED"]
    if failed_all:
        log.info(f"\n🔄 Retrying {len(failed_all)} failed symbols...")
        time.sleep(5)
        for sym in failed_all:
            time.sleep(3)
            is_new = sym in new_symbols
            df = fetch_symbol_history(
                sym,
                period=FULL_LOAD_PERIOD if is_new else None,
                start_date=None if is_new else (watermarks.get(sym, today) + timedelta(days=1)),
            )
            if not df.empty:
                all_frames.append(df)
                max_date = pd.to_datetime(df["Date"]).max()
                symbol_results[sym] = {
                    "last_date": max_date.date() if hasattr(max_date, "date") else max_date,
                    "rows": len(df),
                    "type": "FULL" if is_new else "DELTA",
                    "status": "SUCCESS",
                }
                log.info(f"✅ Recovered {sym}")

    # 8. Write to Delta Lake
    if not all_frames:
        if up_to_date and not new_symbols and not delta_symbols:
            log.info("\n✅ All symbols are up-to-date. Nothing to extract.")
        else:
            log.warning("⚠️  No data fetched at all.")
        return

    combined_df = pd.concat(all_frames, ignore_index=True)
    success_count = len([s for s, r in symbol_results.items() if r["status"] == "SUCCESS"])
    failed_count = len([s for s, r in symbol_results.items() if r["status"] == "FAILED"])

    # MERGE (upsert) — idempotent, no duplicates ever
    write_to_delta(combined_df)

    # 9. Update control table watermarks
    successful_results = {s: r for s, r in symbol_results.items() if r["status"] == "SUCCESS"}
    update_symbol_watermarks(run_id, successful_results)

    # Also update failed symbols so we know they need retry
    failed_results = {s: r for s, r in symbol_results.items() if r["status"] == "FAILED"}
    if failed_results:
        update_symbol_watermarks(run_id, failed_results)

    # 10. Refresh Snowflake External Tables
    log.info("❄️  Refreshing Snowflake External Tables...")
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute("ALTER EXTERNAL TABLE EXT_DAILY_PRICES REFRESH")
        cur.execute("ALTER EXTERNAL TABLE EXT_NIFTY500_MASTER REFRESH")
        cur.close()
        conn.close()
        log.info("   ✅ Refresh complete (data is now queryable)")
    except Exception as e:
        log.warning(f"⚠️  Failed to refresh External Tables: {e}")

    # 11. Summary
    elapsed = time.time() - start_time
    log.info("")
    log.info("═" * 60)
    log.info("🎉 PIPELINE COMPLETE")
    log.info("═" * 60)
    log.info(f"  🆔 Run ID:            {run_id}")
    log.info(f"  ⏱️  Duration:          {elapsed / 60:.1f} minutes")
    log.info(f"  🆕 New symbols:       {len(new_symbols)} (full {FULL_LOAD_PERIOD} backfill)")
    log.info(f"  📈 Delta symbols:     {len(delta_symbols)} (incremental)")
    log.info(f"  ✅ Up-to-date:        {len(up_to_date)} (skipped)")
    log.info(f"  📊 Symbols OK:        {success_count}")
    log.info(f"  ❌ Symbols failed:    {failed_count}")
    log.info(f"  📊 Total rows:        {len(combined_df):,}")
    log.info(f"  📊 Date range:        {combined_df['Date'].min()} → {combined_df['Date'].max()}")
    log.info(f"  📝 Format:            Delta Lake")
    log.info(f"  ☁️  Delta table:       {S3_DELTA_TABLE_URI}")

    if failed_count > 0:
        log.warning(f"\n  ⚠️  Failed symbols:")
        for sym, r in symbol_results.items():
            if r["status"] == "FAILED":
                log.warning(f"      - {sym}")

    log.info("═" * 60)


if __name__ == "__main__":
    main()
