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
    lower((select value.string_value from unnest(event_params) where key = 'page_location')) as page_location,
    lower((select value.string_value from unnest(event_params) where key = 'page_title')) as page_title,
    count(*) as pageviews
  from {{ ref('scr_ga_global_data') }}
  where event_name = 'page_view'
  group by 1,2,3,4,5,6,7,8
  order by event_date desc 
  