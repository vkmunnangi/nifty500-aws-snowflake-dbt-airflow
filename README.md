# Nifty 500 Stock Market Data Pipeline

A production-grade **Lakehouse architecture** for Nifty 500 stock market data, built with modern data engineering tools.

## Architecture

```
yfinance API → Python Extractor → S3 (Delta Lake) → Snowflake (External Tables) → dbt Cloud (Silver/Gold) → Dashboard
```

### Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| **Extraction** | Python + yfinance | Download 5 years of OHLCV data for 500 stocks |
| **Storage** | AWS S3 (Delta Lake) | Single source of truth, columnar format |
| **Compute** | Snowflake | Zero-copy reads via External Tables |
| **Transform** | dbt Cloud | Silver & Gold layer transformations |
| **Orchestration** | Airflow *(planned)* | Daily pipeline scheduling |

### Data Layers

| Layer | Schema | Description |
|-------|--------|-------------|
| **Bronze** | `RAW` | Raw OHLCV data in Delta Lake format on S3 |
| **Silver** | `SILVER` | Cleaned, deduplicated daily + resampled weekly/monthly |
| **Gold** | `GOLD` | Technical indicators (SMA, CCI, RS, Breakouts) |

## Key Features

- **Idempotent writes** — Delta Lake MERGE (upsert) on `symbol + date`, zero duplicates
- **Per-symbol watermarks** — Only fetches new data for existing symbols
- **New symbol detection** — Automatic 5-year backfill for symbols added to the index
- **Fault tolerant** — Exponential backoff retries, atomic commits, VACUUM for orphan cleanup

## Project Structure

```
nifty500-aws-snowflake-dbt-airflow/
├── src/
│   └── extractors/
│       └── yfinance_to_s3.py      # Main extraction pipeline
├── dbt/                            # dbt Cloud project (Silver + Gold)
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── setup_snowflake.py              # Snowflake objects setup
├── create_master_list.py           # Nifty 500 symbol list generator
├── requirements.txt
├── .env.example                    # Required environment variables
└── README.md
```

## Setup

1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your credentials
3. Install dependencies: `pip install -r requirements.txt`
4. Run Snowflake setup: `python setup_snowflake.py`
5. Run extraction: `python src/extractors/yfinance_to_s3.py`

## License

MIT
