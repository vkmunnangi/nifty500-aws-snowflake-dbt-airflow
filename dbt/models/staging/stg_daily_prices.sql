-- models/staging/stg_daily_prices.sql
-- Staging model: Clean, deduplicate, and type-cast raw daily prices.
-- Materialized as a VIEW (no storage cost, always fresh from source).

WITH source AS (
    SELECT
        date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        dividends,
        stock_splits
    FROM {{ source('raw', 'ext_daily_prices') }}
    WHERE symbol IS NOT NULL
      AND date IS NOT NULL
      AND close IS NOT NULL
),

-- Deduplicate: keep the latest row per symbol+date
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date
            ORDER BY volume DESC  -- prefer the row with higher volume if duplicates
        ) AS row_num
    FROM source
)

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    dividends,
    stock_splits
FROM deduplicated
WHERE row_num = 1
