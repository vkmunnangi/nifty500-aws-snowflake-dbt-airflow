-- models/silver/weekly_prices.sql
-- Resample daily OHLCV data into weekly bars (Friday close).
-- Replaces the pandas resample('W-FRI') logic from the old pipeline.

WITH daily AS (
    SELECT * FROM {{ ref('stg_daily_prices') }}
),

-- Assign each trading day to its week-ending Friday
with_week AS (
    SELECT
        *,
        -- DAYOFWEEK: 0=Mon, 6=Sun in Snowflake. Friday = 4.
        -- Calculate the next Friday for each date
        CASE
            WHEN DAYOFWEEK(date) <= 4
                THEN DATEADD('day', 4 - DAYOFWEEK(date), date)
            ELSE
                DATEADD('day', 11 - DAYOFWEEK(date), date)
        END AS week_ending_friday
    FROM daily
),

-- Get the first and last trading day per symbol per week
-- (needed for FIRST open and LAST close)
week_boundaries AS (
    SELECT
        symbol,
        week_ending_friday,
        MIN(date) AS first_day,
        MAX(date) AS last_day
    FROM with_week
    GROUP BY symbol, week_ending_friday
),

-- Get the Open from the first day and Close from the last day
ohlcv AS (
    SELECT
        wb.symbol,
        wb.week_ending_friday AS date,
        first_day_data.open,
        agg.high,
        agg.low,
        last_day_data.close,
        agg.volume
    FROM week_boundaries wb

    -- Aggregated High/Low/Volume for the week
    JOIN (
        SELECT
            symbol,
            week_ending_friday,
            MAX(high) AS high,
            MIN(low) AS low,
            SUM(volume) AS volume
        FROM with_week
        GROUP BY symbol, week_ending_friday
    ) agg
        ON wb.symbol = agg.symbol
        AND wb.week_ending_friday = agg.week_ending_friday

    -- Open from the first trading day of the week
    JOIN daily first_day_data
        ON wb.symbol = first_day_data.symbol
        AND wb.first_day = first_day_data.date

    -- Close from the last trading day of the week
    JOIN daily last_day_data
        ON wb.symbol = last_day_data.symbol
        AND wb.last_day = last_day_data.date
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
