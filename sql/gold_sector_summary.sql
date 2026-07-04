-- gold_sector_summary: average daily return % per sector per date,
-- joining Silver prices to the sector_map reference table (set up once
-- via sql/setup_sector_map.sql). Depends on gold_daily_summary already
-- being rebuilt in this run, since it reuses daily_return_pct from there
-- rather than recomputing LAG() logic a second time.

CREATE OR REPLACE TABLE `{project_id}.gold_stocks.gold_sector_summary` AS
SELECT
  m.sector,
  d.data_date,
  COUNT(DISTINCT d.symbol) AS symbols_counted,
  AVG(d.daily_return_pct) AS avg_daily_return_pct,
  SUM(d.volume) AS total_sector_volume,
  CURRENT_TIMESTAMP() AS gold_processed_at
FROM `{project_id}.gold_stocks.gold_daily_summary` d
JOIN `{project_id}.gold_stocks.sector_map` m
  ON d.symbol = m.symbol
WHERE d.daily_return_pct IS NOT NULL
GROUP BY m.sector, d.data_date
ORDER BY m.sector, d.data_date;