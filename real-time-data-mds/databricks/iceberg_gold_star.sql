-- Databricks SQL: Gold star schema (fact + dims) with ticker metadata
CREATE CATALOG IF NOT EXISTS lakehouse;
CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

-- Dim time
CREATE OR REPLACE TABLE lakehouse.gold.dim_time
USING iceberg
AS
SELECT DISTINCT
  CAST(event_time AS DATE) AS date_day,
  date_trunc('hour', event_time) AS hour_ts,
  hour(event_time) AS hour_of_day,
  day(event_time) AS day_of_month,
  month(event_time) AS month_of_year,
  year(event_time) AS year
FROM lakehouse.silver.stock_quotes;

-- Dim ticker (static metadata stub; update as needed)
CREATE OR REPLACE TABLE lakehouse.gold.dim_ticker
USING iceberg
AS
SELECT * FROM VALUES
  ('AAPL','Apple Inc','Technology','Consumer Electronics','NASDAQ'),
  ('GOOGL','Alphabet Inc','Communication Services','Internet Content & Information','NASDAQ'),
  ('MSFT','Microsoft Corp','Technology','Software - Infrastructure','NASDAQ'),
  ('AMZN','Amazon.com Inc','Consumer Cyclical','Internet Retail','NASDAQ'),
  ('TSLA','Tesla Inc','Consumer Cyclical','Auto Manufacturers','NASDAQ')
AS t(ticker, company_name, sector, industry, exchange);

-- Fact table
CREATE OR REPLACE TABLE lakehouse.gold.fact_stock_quote
USING iceberg
AS
SELECT
  s.event_id,
  s.source_system,
  s.ticker,
  s.current_price,
  s.high_price,
  s.low_price,
  s.open_price,
  s.previous_close_price,
  s.event_time,
  d.date_day,
  d.hour_ts
FROM lakehouse.silver.stock_quotes s
LEFT JOIN lakehouse.gold.dim_time d
  ON d.hour_ts = date_trunc('hour', s.event_time);
