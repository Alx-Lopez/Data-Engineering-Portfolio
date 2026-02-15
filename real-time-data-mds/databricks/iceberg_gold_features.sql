-- Databricks SQL: build Gold features for 1-hour price prediction
CREATE CATALOG IF NOT EXISTS lakehouse;
CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

CREATE OR REPLACE TABLE lakehouse.gold.stock_quote_features_1h
USING iceberg
PARTITIONED BY (dt, hr)
AS
WITH base AS (
  SELECT
    *,
    LAG(current_price, 1) OVER (PARTITION BY ticker ORDER BY event_time) AS price_lag_1,
    LAG(current_price, 5) OVER (PARTITION BY ticker ORDER BY event_time) AS price_lag_5,
    LAG(current_price, 10) OVER (PARTITION BY ticker ORDER BY event_time) AS price_lag_10,
    LEAD(current_price, 60) OVER (PARTITION BY ticker ORDER BY event_time) AS price_fwd_60
  FROM lakehouse.silver.stock_quotes
),
returns AS (
  SELECT
    *,
    CASE
      WHEN price_lag_1 IS NULL OR price_lag_1 = 0 THEN NULL
      ELSE (current_price - price_lag_1) / price_lag_1
    END AS return_1
  FROM base
),
features AS (
  SELECT
    *,
    AVG(return_1) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) AS return_mean_15,
    STDDEV(return_1) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) AS return_std_15,
    AVG(return_1) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS return_mean_60,
    STDDEV(return_1) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS return_std_60,
    MAX(current_price) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS price_max_60,
    MIN(current_price) OVER (PARTITION BY ticker ORDER BY event_time ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS price_min_60
  FROM returns
)
SELECT
  event_id,
  source_system,
  ticker,
  current_price,
  high_price,
  low_price,
  open_price,
  previous_close_price,
  event_ts,
  event_time,
  dt,
  hr,
  price_lag_1,
  price_lag_5,
  price_lag_10,
  return_mean_15,
  return_std_15,
  return_mean_60,
  return_std_60,
  price_max_60,
  price_min_60,
  price_fwd_60 AS label_price_1h
FROM features;
