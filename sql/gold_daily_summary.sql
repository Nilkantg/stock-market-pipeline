-- gold_daily_summary: one row per symbol per date, with OHLCV plus
-- daily return % and day-over-day volume change %.
--
-- Rebuilt fully each run (CREATE OR REPLACE) from all valid Silver history.
-- Safe to re-run any number of times — always reflects current Silver state.

CREATE OR REPLACE TABLE `{project_id}.gold_stocks.gold_daily_summary` AS
WITH base AS (
  SELECT
    symbol,
    data_date,
    open,
    high,
    low,
    close,
    volume
  FROM `{project_id}.silver_stocks.daily_prices`
  WHERE is_valid = TRUE
),
with_prev AS (
  SELECT
    symbol,
    data_date,
    open,
    high,
    low,
    close,
    volume,
    LAG(close) OVER (PARTITION BY symbol ORDER BY data_date) AS prev_close,
    LAG(volume) OVER (PARTITION BY symbol ORDER BY data_date) AS prev_volume
  FROM base
)
SELECT
  symbol,
  data_date,
  open,
  high,
  low,
  close,
  volume,
  prev_close,
  -- Daily return %: null on a symbol's first day (no prior close to compare)
  SAFE_DIVIDE(close - prev_close, prev_close) * 100 AS daily_return_pct,
  prev_volume,
  SAFE_DIVIDE(volume - prev_volume, prev_volume) * 100 AS volume_change_pct,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM with_prev
ORDER BY symbol, data_date;