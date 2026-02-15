-- Databricks SQL: build Silver (deduped, typed)
CREATE CATALOG IF NOT EXISTS lakehouse;
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

CREATE OR REPLACE TABLE lakehouse.silver.stock_quotes
USING iceberg
PARTITIONED BY (dt, hr)
AS
SELECT
  event_id,
  source_system,
  ticker,
  CAST(current_price AS DOUBLE) AS current_price,
  CAST(high_price AS DOUBLE) AS high_price,
  CAST(low_price AS DOUBLE) AS low_price,
  CAST(open_price AS DOUBLE) AS open_price,
  CAST(previous_close_price AS DOUBLE) AS previous_close_price,
  CAST(timestamp AS BIGINT) AS event_ts,
  to_timestamp(from_unixtime(CAST(timestamp AS BIGINT))) AS event_time,
  to_date(from_unixtime(CAST(timestamp AS BIGINT))) AS dt,
  date_format(from_unixtime(CAST(timestamp AS BIGINT)), 'HH') AS hr
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY event_id, source_system
           ORDER BY CAST(timestamp AS BIGINT) DESC
         ) AS rn
  FROM lakehouse.bronze.stock_quotes
)
WHERE rn = 1;
