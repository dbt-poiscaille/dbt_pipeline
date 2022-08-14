{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select distinct
    event_date,
    device.category as device_category,
    user_pseudo_id, 
    (select value.string_value from unnest(event_params) where key = 'checkout_category') as checkout_category,
    (select value.string_value from unnest(event_params) where key = 'event_action') as event_action,
    (select value.string_value from unnest(event_params) where key = 'event_label') as event_label
    FROM {{ ref('scr_ga_global_data') }}
 where event_name = 'screenInteraction'   