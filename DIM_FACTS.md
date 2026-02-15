# Dimensional Model Write‑Up

This project uses a simple star schema in the Gold layer to support BI and analytics. The model is implemented in Snowflake via dbt and mirrored in Databricks (Iceberg).

## Fact Table

### `fact_stock_quote`
**Grain:** One row per event (quote) per ticker.  
**Primary keys:** `event_id`, `source_system`  
**Purpose:** Central fact table for price analytics and downstream marts.

**Key columns**
- `event_id` (string): deterministic event key from ingestion  
- `source_system` (string): source identifier (e.g., finnhub)  
- `ticker` (string): stock symbol  
- `current_price`, `high_price`, `low_price`, `open_price`, `previous_close_price`  
- `event_time` (timestamp): quote timestamp  
- `date_day`, `hour_ts` (time dimensions joined from `dim_time`)  

## Dimension Tables

### `dim_time`
**Grain:** One row per hour/date derived from event timestamps.  
**Purpose:** Enables time-based rollups and consistent filtering.

**Key columns**
- `date_day` (date)  
- `hour_ts` (timestamp)  
- `hour_of_day`, `day_of_month`, `month_of_year`, `year`  

### `dim_ticker` (SCD2)
**Grain:** One row per ticker version (slowly changing).  
**Purpose:** Stores metadata for tickers, supports historical changes.

**Key columns**
- `ticker`  
- `company_name`, `sector`, `industry`, `exchange`  
- `valid_from`, `valid_to`, `is_current` (SCD2 fields)  

## Source and Lineage
Upstream lineage is captured in Bronze tables:
- `file_name`, `ingest_ts`, `load_batch_id`  
- `_kafka_partition`, `_kafka_offset`  

These allow traceability from Gold facts back to the original Kafka message and file.

## Where These Live
- **Snowflake (dbt):** `dbt_stocks/models/gold/dim_time.sql`, `dim_ticker.sql`, `fact_stock_quote.sql`  
- **Databricks (Iceberg):** `databricks/iceberg_gold_star.sql`  
