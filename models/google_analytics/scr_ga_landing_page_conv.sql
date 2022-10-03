{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select 
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_name,
    device.category as device_category,
    traffic_source.name,
    traffic_source.source,
    traffic_source.medium,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
    lower((select value.string_value from unnest(event_params) where key = 'page_location')) as page_location,
  from {{ ref('scr_ga_global_data') }}
  where event_name = 'session_start'
  order by event_date desc 