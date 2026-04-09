-- models/gold/weekly_technical_analysis.sql
-- Gold Layer: Technical indicators for stock analysis.
-- Replaces the entire calculate_indicators.py from the old pipeline.
--
-- Indicators calculated:
--   1. SMA 30 (Simple Moving Average)
--   2. CCI 34 (Commodity Channel Index)
--   3. RS vs NIFTY 50 (Relative Strength)
--   4. RS vs NIFTY 500 (Relative Strength)
--   5. Breakout 13W (3-month high breakout)
--   6. Breakout 26W (6-month high breakout)
--   7. Breakout 52W (52-week high breakout)
--   8. Breakout 104W (2-year high breakout)

WITH weekly AS (
    SELECT * FROM {{ ref('weekly_prices') }}
),

-- ═══════════════════════════════════════════════════════
-- BENCHMARK DATA (for Relative Strength calculation)
-- ═══════════════════════════════════════════════════════
-- Extract NIFTY 50 and NIFTY 500 weekly closes
-- These act as the "market" baseline for RS calculation

benchmarks AS (
    SELECT
        date,
        MAX(CASE WHEN symbol = 'NIFTY50' THEN close END)  AS nifty50_close,
        MAX(CASE WHEN symbol = 'NIFTY500' THEN close END) AS nifty500_close
    FROM weekly
    GROUP BY date
),

-- ═══════════════════════════════════════════════════════
-- CORE INDICATORS (all via Window Functions)
-- ═══════════════════════════════════════════════════════

indicators AS (
    SELECT
        w.date,
        w.symbol,
        w.open,
        w.high,
        w.low,
        w.close,
        w.volume,

        -- ── SMA 30 ──────────────────────────────────────
        -- Simple Moving Average over last 30 weekly closes
        AVG(w.close) OVER (
            PARTITION BY w.symbol
            ORDER BY w.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30,

        -- Count of rows in SMA window (to filter incomplete windows)
        COUNT(*) OVER (
            PARTITION BY w.symbol
            ORDER BY w.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30_count,

        -- ── CCI 34 ─────────────────────────────────────
        -- Commodity Channel Index = (TP - SMA_TP) / (0.015 * MAD_TP)
        -- Typical Price
        (w.high + w.low + w.close) / 3.0 AS typical_price,

        -- SMA of Typical Price over 34 periods
        AVG((w.high + w.low + w.close) / 3.0) OVER (
            PARTITION BY w.symbol
            ORDER BY w.date
            ROWS BETWEEN 33 PRECEDING AND CURRENT ROW
        ) AS tp_sma_34,

        -- Count for CCI window
        COUNT(*) OVER (
            PARTITION BY w.symbol
            ORDER BY w.date
            ROWS BETWEEN 33 PRECEDING AND CURRENT ROW
        ) AS cci_count,

        -- ── RELATIVE STRENGTH ──────────────────────────
        -- RS = Stock Close / Benchmark Close
        -- Higher RS = stock outperforming the market
        CASE
            WHEN b.nifty50_close IS NOT NULL AND b.nifty50_close > 0
            THEN ROUND(w.close / b.nifty50_close, 6)
        END AS rs_nifty50,

        CASE
            WHEN b.nifty500_close IS NOT NULL AND b.nifty500_close > 0
            THEN ROUND(w.close / b.nifty500_close, 6)
        END AS rs_nifty500,

        -- ── BREAKOUT DETECTION ─────────────────────────
        -- Is today's close higher than the highest high
        -- of the previous N weeks? (excludes current week)

        -- 13-Week Breakout (3 months)
        CASE
            WHEN w.close > MAX(w.high) OVER (
                PARTITION BY w.symbol
                ORDER BY w.date
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) THEN 1 ELSE 0
        END AS is_breakout_13w,

        -- 26-Week Breakout (6 months)
        CASE
            WHEN w.close > MAX(w.high) OVER (
                PARTITION BY w.symbol
                ORDER BY w.date
                ROWS BETWEEN 26 PRECEDING AND 1 PRECEDING
            ) THEN 1 ELSE 0
        END AS is_breakout_26w,

        -- 52-Week Breakout (1 year)
        CASE
            WHEN w.close > MAX(w.high) OVER (
                PARTITION BY w.symbol
                ORDER BY w.date
                ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING
            ) THEN 1 ELSE 0
        END AS is_breakout_52w,

        -- 104-Week Breakout (2 years)
        CASE
            WHEN w.close > MAX(w.high) OVER (
                PARTITION BY w.symbol
                ORDER BY w.date
                ROWS BETWEEN 104 PRECEDING AND 1 PRECEDING
            ) THEN 1 ELSE 0
        END AS is_breakout_104w

    FROM weekly w
    LEFT JOIN benchmarks b ON w.date = b.date
    -- Exclude the benchmark symbols themselves from the output
    WHERE w.symbol NOT IN ('NIFTY50', 'NIFTY500')
)

-- ═══════════════════════════════════════════════════════
-- FINAL OUTPUT
-- ═══════════════════════════════════════════════════════

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume,

    -- SMA 30 (only show when we have a full 30-week window)
    CASE WHEN sma_30_count >= 30 THEN ROUND(sma_30, 2) END AS sma_30,

    -- CCI 34 = (TP - SMA_TP) / (0.015 * SMA_TP)
    -- Simplified CCI (using SMA as proxy for Mean Absolute Deviation)
    CASE
        WHEN cci_count >= 34 AND tp_sma_34 > 0
        THEN ROUND((typical_price - tp_sma_34) / NULLIF(0.015 * tp_sma_34, 0), 2)
    END AS cci_34,

    rs_nifty50,
    rs_nifty500,
    is_breakout_13w,
    is_breakout_26w,
    is_breakout_52w,
    is_breakout_104w

FROM indicators
-- Only include rows where SMA has enough data (at least 30 weeks of history)
WHERE sma_30_count >= 30
ORDER BY symbol, date
