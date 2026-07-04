-- One-time setup: sector reference table.
-- Run manually once (see Phase 3 verify steps) — this is reference/dimension
-- data, not something the daily DAG needs to regenerate every run.
-- Lives in gold_stocks since it's Gold-layer supporting metadata, not raw or
-- cleaned transaction data.

CREATE TABLE IF NOT EXISTS `{project_id}.gold_stocks.sector_map` (
  symbol STRING NOT NULL,
  sector STRING NOT NULL
);

-- Idempotent upsert of the mapping. Using MERGE instead of INSERT so
-- re-running this file never creates duplicates.
MERGE `{project_id}.gold_stocks.sector_map` T
USING (
  SELECT 'RELIANCE.NS' AS symbol, 'Energy' AS sector UNION ALL
  SELECT 'TCS.NS', 'IT Services' UNION ALL
  SELECT 'INFY.NS', 'IT Services' UNION ALL
  SELECT 'HDFCBANK.NS', 'Financial Services' UNION ALL
  SELECT 'WIPRO.NS', 'IT Services'
) S
ON T.symbol = S.symbol
WHEN MATCHED THEN UPDATE SET sector = S.sector
WHEN NOT MATCHED THEN INSERT (symbol, sector) VALUES (S.symbol, S.sector);