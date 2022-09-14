{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_locker as (
select 
  sale_id,
  shippingat,
  DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
  DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
  LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,       
  subscription_status,
  offerings_value_price_ttc,
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
  name
from
 {{ ref('src_mongodb_sale') }}
 --where sale_id = '62c28bbbb9c4380fdbeecfe7'
) , 

final_data as (
select 
     sale_id, 
     email,
     user_id,
     name  , 
     sale_date,
     first_day_week,
     last_day_week,
     subscription_status,
     subscription_price,
     subscription_id,
     sale_type,
     margin,
     margin__fl, 
     case 
         when margin is not null and margin__fl is null then margin 
         when margin__fl is not null and margin is null then margin__fl
         end as margin_final,    
     offerings_value_channel,     
     round(sum(price_ttc)/100,2) as price_ttc, 
     round(sum(price_ht)/100,2) as price_ht,      
     sum(offerings_value_price_ttc/100) as offerings_value_price_ttc ,
     --sum(offerings_value_price_ht/100) as offerings_value_price_ht,     
     --sum(offerings_value_item_value_cost_ht/100) as offerings_value_item_value_cost_ht ,
     --sum(offerings_value_items_value_cost_ttc/100) as offerings_value_items_value_cost_ttc,     
     --offerings_value_count,
     --offerings_value_price_tax,
     --offerings_value_name,
     --offerings_value_items_value_portion_unit,
     --offerings_value_items_value_portion_quantity,
     --offerings_value_items_value_cost_unit,
     --offerings_value_items_value_product_name
 from data_locker
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) ,

consolidation as (
select 
     sale_id, 
     email,
     user_id,
     name  , 
     sale_date, 
     extract(week from sale_date) as week_number, 
     extract(month from sale_date) as month, 
     subscription_status,
     subscription_price,
     subscription_id,
     sale_type,
     margin_final, 
     offerings_value_channel,
     case 
         when margin_final is null then null
         when margin_final is not null then ((100 - margin_final)/100) * (price_ttc/1.055)
         end as cogs,
     price_ttc, 
     price_ht
  from final_data   
) ,

bonus_data as (
select 
     sale_id,
     sale_date,  
     user_id,
     name  , 
     extract(week from sale_date) as week_number, 
     extract(month from sale_date) as month, 
     subscription_status,
     subscription_price,
     subscription_id,
     margin_final, 
     offerings_value_price_ttc
from final_data
WHERE subscription_id IS NOT NULL
  AND offerings_value_channel ='shop'     
) , 
bonus_consolidation as (
select 
   sale_date as sale_date_bonus, 
   round(count (distinct sale_id),2) as nb_bonus,
   round(sum(offerings_value_price_ttc)/1.055,2) as bonus_revenue, 
  from bonus_data
  group by 1
) , 

final_order as (
select
    sale_date, 
    DATE_TRUNC(sale_date, WEEK(MONDAY)) as first_day_week,
    LAST_DAY(sale_date, WEEK(MONDAY)) as last_day_week,       
    week_number, 
    month, 
    count(distinct case when sale_type ='order' then sale_id end ) as nb_order, 
    count(distinct case when sale_type ='locker' then sale_id end ) as nb_locker,
    count(distinct case when sale_type ='order' then user_id end ) as nb_customer_order, 
    count(distinct case when sale_type ='locker' then user_id end ) as nb_customer_locker,     
    round(sum( case when sale_type ='order' then price_ttc end )/1.055,2) as order_revenue_ttc, 
    round(sum( case when sale_type ='locker' then price_ttc end )/1.055,2) as locker_revenue_ttc,
    round(sum( case when sale_type ='order' then price_ht end ),2) as order_revenue_ht, 
    round(sum( case when sale_type ='locker' then price_ht end ),2) as locker_revenue_ht,
    round(sum( case when sale_type ='order' then cogs end ),2) as order_cogs, 
    round(sum( case when sale_type ='locker' then cogs end ),2) as locker_cogs
    from consolidation
    group by 1,2,3,4,5        
) , 
coupon_refund as (
  select 
   in_date,
   nb_customer_cp,
   total_amount_cp,
   charge_date,
   amount_refunded
   from {{ ref('stg_coupon_refund_consolidation') }}
)

select 
  * from final_order
  left join bonus_consolidation
  on final_order.sale_date = bonus_consolidation.sale_date_bonus
  left join coupon_refund
  on final_order.sale_date = coupon_refund.in_date

order by final_order.sale_date desc  
















     

   