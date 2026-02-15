{{ config(materialized='table') }}

select distinct
  cast(event_time as date) as date_day,
  date_trunc('hour', event_time) as hour_ts,
  extract(hour from event_time) as hour_of_day,
  extract(day from event_time) as day_of_month,
  extract(month from event_time) as month_of_year,
  extract(year from event_time) as year
from {{ ref('silver_stock_quotes') }}
