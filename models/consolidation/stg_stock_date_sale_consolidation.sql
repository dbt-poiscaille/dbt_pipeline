{{
  config(
    materialized = 'table',
    labels = {'type': 'mongo', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

SELECT
  stock_id,
  supplier_name,
  supplier_boat_name,
  sale_date,
  offerings_value_channel,
  channel,
  type_sale,
  sale_product_type,
  sale_product_name,
  offerings_value_items_value_portion_unit as portion_unit,

  sum(offerings_value_items_value_portion_quantity) as portion_quantity,
  round(sum(offerings_value_items_value_cost_ttc)/100,2) as cost_ttc,

FROM {{ ref('stg_sale_offering_stock_consolidation')}} 
group by 1,2,3,4,5,6,7,8,9,10
order by sale_date desc