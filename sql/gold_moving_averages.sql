-- gold_moving_averages: 7-day and 30-day moving average of closing price
-- per symbol. Uses a window frame of "current row + N-1 preceding" so early
-- rows (before 7 or 30 days of history exist) get a partial-window average
-- rather than null — we flag those via *_is_partial columns instead of
-- hiding them, so downstream consumers know to treat early values with care.

CREATE OR REPLACE TABLE `{project_id}.gold_stocks.gold_moving_averages` AS
WITH base AS (
  SELECT
    symbol,
    data_date,
    close
  FROM `{project_id}.silver_stocks.daily_prices`
  WHERE is_valid = TRUE
),
windowed AS (
  SELECT
    symbol,
    data_date,
    close,
    AVG(close) OVER (
      PARTITION BY symbol ORDER BY data_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7day,
    COUNT(*) OVER (
      PARTITION BY symbol ORDER BY data_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7day_window_size,
    AVG(close) OVER (
      PARTITION BY symbol ORDER BY data_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30day,
    COUNT(*) OVER (
      PARTITION BY symbol ORDER BY data_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30day_window_size
  FROM base
)
SELECT
  symbol,
  data_date,
  close,
  ma_7day,
  ma_7day_window_size < 7 AS ma_7day_is_partial,
  ma_30day,
  ma_30day_window_size < 30 AS ma_30day_is_partial,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM windowed
ORDER BY symbol, data_date;