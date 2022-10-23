{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
    sale_date,
    sale_id,
    sale_price_ttc,
    type_sale,
    subscription_price,
    subscription_type,
    offerings_value_id,
    offerings_value_name,
    suppliername,
    items_value_product_type,
    items_value_product_name,
    offerings_value_items_value_portion_unit,

    sum(offerings_value_items_value_portion_quantity) as total_slicing_portion_quantity,
    sum(offerings_value_items_value_cost_ttc) as total_items_value_allocations_value_cost_ttc

from {{ ref('stg_mongo_sale_offering_consolidation') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by sale_date desc, sale_id
