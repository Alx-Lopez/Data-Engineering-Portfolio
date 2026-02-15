{{ config(materialized='incremental', unique_key=['event_id','source_system'], incremental_strategy='merge') }}

select
  event_id,
  source_system,
  ticker,
  try_cast(current_price as float) as current_price,
  try_cast(high_price as float) as high_price,
  try_cast(low_price as float) as low_price,
  try_cast(open_price as float) as open_price,
  try_cast(previous_close_price as float) as previous_close_price,
  to_timestamp(event_ts) as event_time,
  cast(event_time as date) as trade_date,
  date_trunc('hour', event_time) as trade_hour
from (
  select
    data:event_id::string as event_id,
    data:source_system::string as source_system,
    data:ticker::string as ticker,
    data:current_price::float as current_price,
    data:high_price::float as high_price,
    data:low_price::float as low_price,
    data:open_price::float as open_price,
    data:previous_close_price::float as previous_close_price,
    to_timestamp(data:timestamp::number) as event_ts
  from {{ source('raw', 'bronze_stock_quotes') }}
) s
where event_id is not null
{% if is_incremental() %}
  and event_ts >= (select coalesce(max(event_time), '1970-01-01') from {{ this }})
{% endif %}
