{{
  config(
    materialized = 'table',
    labels = {'type': 'mongo', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

select
    distinct
    DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
    sale_id, 
    price_ttc,
    subscription_price,
    offerings_value_channel,
    channel,
    type_sale,
    offerings_value_id, 
    offerings_value_name, 
    offerings_value_count, 
    offerings_value_price_ttc,
    offerings_value_items_value_portion_unit, 
    offerings_value_items_value_portion_quantity, 
    offerings_value_items_value_cost_ttc, 
    offerings_value_items_value_cost_unit,
from {{ ref('src_mongodb_sale')}}
