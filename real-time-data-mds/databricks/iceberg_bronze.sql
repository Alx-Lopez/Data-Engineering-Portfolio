-- Databricks SQL stub: create Iceberg bronze table on S3
-- Update catalog/schema/bucket before running.

CREATE CATALOG IF NOT EXISTS lakehouse;
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.stock_quotes
USING iceberg
PARTITIONED BY (dt, hr)
LOCATION 's3://brz-mds/bronze/stock-quotes/'
AS
SELECT
  event_id,
  source_system,
  ticker,
  current_price,
  high_price,
  low_price,
  open_price,
  previous_close_price,
  timestamp,
  to_date(from_unixtime(timestamp)) AS dt,
  date_format(from_unixtime(timestamp), 'HH') AS hr
FROM parquet.`s3://brz-mds/topic=stock-quotes/`;
