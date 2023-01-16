{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

with data_info as (
select distinct
    event_date,
    device.category as device_category,
    user_pseudo_id, 
    event_name,
    traffic_source.name as traffic_name, 
    traffic_source.source as traffic_source, 
    traffic_source.medium as traffic_medium,     
    (select value.string_value from unnest(event_params) where key = 'checkout_category') as checkout_category,
    (select value.string_value from unnest(event_params) where key = 'event_action') as event_action,
    (select value.string_value from unnest(event_params) where key = 'event_label') as event_label,
    (select value.string_value from unnest(event_params) where key = 'subscription_rate') as subscription_rate    
    
    FROM {{ ref('scr_ga_global_data') }}
 where event_name in ('screenInteraction','orderComplete', 'addToCart', 'checkout') 
 --and (select value.string_value from unnest(event_params) where key = 'checkout_category') = 'subscriptionFunnel'
 order by event_date desc 
)

select 
  * from data_info
  where checkout_category='giftcardFunnel' and event_name = 'orderComplete'
  order by event_date desc 