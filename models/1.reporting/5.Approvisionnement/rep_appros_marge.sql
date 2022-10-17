{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
  sale_date,
  sale_id,
  type_sale,
  subscription_price,
  offerings_value_name,

  max(sale_price_ttc) as sale_price_ttc,
  sum(total_items_value_allocations_value_cost_ttc) as total_cost_ttc,
  round((max(sale_price_ttc) - sum(total_items_value_allocations_value_cost_ttc))/max(sale_price_ttc),2) as gross_margin

from {{ ref('rep_appros') }}
group by 1,2,3,4,5
order by sale_date desc, sale_id