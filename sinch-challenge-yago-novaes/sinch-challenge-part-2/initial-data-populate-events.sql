---- source

with events as (

  select
    created_at,
    traffic_source,
    postal_code,
    session_id,
    event_type 
  from bigquery-public-data.thelook_ecommerce.events

)
,

--- agreggate by minimal granularity

events_aggregated as (

  select
    timestamp_trunc(created_at, hour) as created_at,
    traffic_source,
    postal_code,
    count(distinct session_id) as sessions,
    countif(event_type = 'purchase') as purchase_events,
    count(*) as total_events
  from events
  where cast(created_at as date) >= date_sub(current_date(), interval 2 year)
  group by created_at, traffic_source, postal_code

)

-- select statement

select * 
from events_aggregated
order by created_at desc;