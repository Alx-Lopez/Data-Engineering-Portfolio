{{ config(materialized='table') }}

select
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
from {{ ref('silver_stock_quotes') }} s
left join {{ ref('dim_time') }} d
  on d.hour_ts = date_trunc('hour', s.event_time)
