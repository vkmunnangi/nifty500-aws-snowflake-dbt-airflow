-- models/silver/monthly_prices.sql
-- Resample daily OHLCV data into monthly bars.
-- Replaces the pandas resample('ME') logic from the old pipeline.

WITH daily AS (
    SELECT * FROM {{ ref('stg_daily_prices') }}
),

-- Assign each trading day to its month-end
with_month AS (
    SELECT
        *,
        LAST_DAY(date) AS month_end
    FROM daily
),

-- Get the first and last trading day per symbol per month
month_boundaries AS (
    SELECT
        symbol,
        month_end,
        MIN(date) AS first_day,
        MAX(date) AS last_day
    FROM with_month
    GROUP BY symbol, month_end
),

-- Combine: Open from first day, Close from last day, aggregated H/L/V
ohlcv AS (
    SELECT
        mb.symbol,
        mb.last_day AS date,
        first_day_data.open,
        agg.high,
        agg.low,
        last_day_data.close,
        agg.volume
    FROM month_boundaries mb

    JOIN (
        SELECT
            symbol,
            month_end,
            MAX(high) AS high,
            MIN(low) AS low,
            SUM(volume) AS volume
        FROM with_month
        GROUP BY symbol, month_end
    ) agg
        ON mb.symbol = agg.symbol
        AND mb.month_end = agg.month_end

    JOIN daily first_day_data
        ON mb.symbol = first_day_data.symbol
        AND mb.first_day = first_day_data.date

    JOIN daily last_day_data
        ON mb.symbol = last_day_data.symbol
        AND mb.last_day = last_day_data.date
)

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv
ORDER BY symbol, date
