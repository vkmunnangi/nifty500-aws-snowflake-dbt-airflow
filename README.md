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
| **Orchestration** | AWS MWAA (Airflow) | Daily pipeline scheduling & dbt API triggers |

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
├── dags/
│   └── stock_pipeline_dag.py       # Airflow orchestration DAG
├── src/
│   └── extractors/
│       └── yfinance_to_s3.py       # Main extraction python script
├── dbt/                            # dbt Cloud project (Silver + Gold)
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── .github/
│   └── workflows/
│       └── mwaa_sync.yml           # CI/CD sync script for Amazon S3
├── mwaa_requirements.txt           # AWS MWAA Airflow dependencies
└── README.md
```

## Setup & Deployment (AWS MWAA)

This project uses **Apache Airflow** hosted on **AWS MWAA** for continuous cloud orchestration.

### 1. CI/CD Integration (GitHub Actions)
You do not need to manually drag and drop files into your MWAA environment.
1. Add your AWS Credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and `MWAA_S3_BUCKET`) to your **GitHub Repository Secrets**.
2. Any code pushed to the `main` branch will automatically be synced to your MWAA S3 bucket via GitHub Actions.

### 2. AWS MWAA Configuration
Inside the Airflow Web UI for your MWAA environment, strictly securely store your variables:

**Airflow Variables:**
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET_NAME`
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

**Airflow Connections:**
- `dbt_cloud_default` (dbt Cloud connection with your API token and Account ID)


## License

MIT
