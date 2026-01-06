select
  data:event_id::string as event_id
  ,data:source_system::string as source_system
  ,data:current_price::float as current_price
  ,null::float as change_amount
  ,null::float as change_percent
  ,data:high_price::float as day_high
  ,data:low_price::float as day_low
  ,data:open_price::float as day_open
  ,data:previous_close_price::float as prev_close
  ,to_timestamp(data:timestamp::number) as market_timestamp
  ,data:ticker::string as ticker
  ,null::timestamp as fetched_at
from {{ source('raw', 'bronze_stock_quotes') }}
