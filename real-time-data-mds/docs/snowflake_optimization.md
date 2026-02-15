# Snowflake Optimization Guide

## Goal
Keep COPY/merge fast, reduce scan cost, and improve BI query latency.

## File sizing (before Snowflake load)
Target Parquet file size:
- 128MB–512MB

If you see thousands of tiny files, run compaction before COPY or batch writes longer.

## Clustering strategy
Use clustering only when tables are large and queries filter on the keys.

Recommended for quotes:
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES
CLUSTER BY (TO_DATE(INGEST_TS), TICKER);
```

If queries mostly filter by time:
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES
CLUSTER BY (TO_DATE(INGEST_TS));
```

Enable auto‑clustering if necessary:
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES
SET AUTO_CLUSTERING = TRUE;
```

## COPY batching
Limit loads to hourly prefixes to prevent reprocessing:
```sql
COPY INTO STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW (data, load_ts, file_name, ingest_ts, load_batch_id)
FROM (
  SELECT $1, CURRENT_TIMESTAMP(), METADATA$FILENAME, CURRENT_TIMESTAMP(), '<batch-id>'
  FROM @bronze_stock_quotes_stage
)
FILE_FORMAT = (TYPE=PARQUET)
PATTERN = '.*dt=2026-01-07/hr=18/.*\\.parquet';
```

## Volume anomaly checks
```sql
SELECT date_trunc('hour', load_ts) AS hour, COUNT(*) AS rows
FROM STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW
GROUP BY 1
ORDER BY 1 DESC;
```

## When to revisit
- COPY runtimes grow or BI dashboards slow down
- Query history shows high scan volume
