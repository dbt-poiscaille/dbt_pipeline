
{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel', 'contains_pie': 'no', 'category':'source'}  
   )
}}


with data_sale as (
select 
  distinct 
  sale_id,
  shippingat,
  DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
  DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
  LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,       
  subscription_status,
  --offerings_value_price_ttc,
  --offerings_value_price_tax,
  --offerings_value_price_ht,
  subscription_price,
  subscription_id,
  case when subscription_id is null then 'order' else 'locker' end as sale_type,
  margin,
  margin__fl, 
  price_ttc, 
  price_ht, 
  offerings_value_channel,
  --offerings_value_count,
  --offerings_value_name,
  --offerings_value_items_value_portion_unit,
  --offerings_value_items_value_portion_quantity,
  --offerings_value_item_value_cost_ht,
  --offerings_value_items_value_cost_ttc,
  --offerings_value_items_value_cost_unit,
  --offerings_value_items_value_product_name,
  email,
  user_id,
  name, 
from
 {{ ref('src_mongodb_sale') }}
) , 

data_consolidation as (
select 
   *, 
    RANK() OVER (PARTITION BY user_id ORDER BY sale_date ) AS customer_seq
  from data_sale   
  where sale_type ='locker'
) 

select 
    data_consolidation.sale_date, 
    count (distinct case when customer_seq=1 then user_id end ) as new_customers,
    count (distinct case when customer_seq>1 then user_id end ) as old_customers
    from data_consolidation 
  group by 1 
  order by sale_date desc 