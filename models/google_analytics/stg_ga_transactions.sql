{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select distinct
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    device.category as device_category,
    user_pseudo_id, 
    traffic_source.name as traffic_name, 
    traffic_source.source as traffic_source, 
    traffic_source.medium as traffic_medium,     
    user_id , 
    (select value.string_value from unnest(event_params) where key = 'checkout_category') as checkout_category,
    (select value.string_value from unnest(event_params) where key = 'event_action') as event_action,
    (select value.string_value from unnest(event_params) where key = 'event_label') as event_label,
    (select value.string_value from unnest(event_params) where key = 'subscription_rate') as subscription_rate,    
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id ,   
    (select value.string_value from unnest(event_params) where key = 'transaction_id') as transaction_id ,
    ecommerce.purchase_revenue , 
    ecommerce.transaction_id as ec_transaction, 
    items.item_id , 

 from {{ ref('scr_ga_global_data') }}, 
 unnest(items) as items 
 where event_name='purchase'
 --and (select value.string_value from unnest(event_params) where key = 'checkout_category') = 'subscriptionFunnel'
 order by event_date desc 