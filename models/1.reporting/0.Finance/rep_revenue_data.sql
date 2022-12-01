{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_locker as (
select
  distinct
  sale_id,
  shippingat,
  offerings_value_channel,
  channel, 
  DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
  DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
  LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,       
  subscription_status,
  offerings_value_price_ttc,
  --offerings_value_price_tax,
  offerings_value_price_ht,
  subscription_price,
  subscription_id,
  type_sale as sale_type,
  margin,
  margin__fl, 
  price_ttc, 
  price_ht, 
  offerings_value_count,
  offerings_value_name,
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
) , 

final_data as (
select 
    distinct 
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
     offerings_value_count,
    offerings_value_name,
     round(sum(offerings_value_price_ttc)/100,2) as price_ttc, 
     round(sum(offerings_value_price_ht)/100,2) as price_ht,      
     sum(offerings_value_price_ttc/100) as offerings_value_price_ttc ,

     --sum(offerings_value_price_ht/100) as offerings_value_price_ht,     
     --sum(offerings_value_item_value_cost_ht/100) as offerings_value_item_value_cost_ht ,
     --sum(offerings_value_items_value_cost_ttc/100) as offerings_value_items_value_cost_ttc,     
     --offerings_value_price_tax,
     --offerings_value_items_value_portion_unit,
     --offerings_value_items_value_portion_quantity,
     --offerings_value_items_value_cost_unit,
     --offerings_value_items_value_product_name
 from data_locker
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
     price_ht,
     offerings_value_count
  from final_data   
) ,

bonus_data as (
select
  sale_id,
  sale_date,  
  user_id,
  name, 
  extract(week from sale_date) as week_number, 
  extract(month from sale_date) as month, 
  subscription_status,
  subscription_price,
  subscription_id,
  count(distinct case when sale_type ='Abonnement' then sale_id end  ) as locker, 
  count(distinct case when sale_type ='Petit plus' then sale_id end  ) as petitplus,
  max( case when sale_type ='Abonnement' then subscription_price end  ) as locker_price, 
  max(case when sale_type = 'Petit plus'then price_ttc else 0 end)  as total_price,
  max(case when sale_type = 'Petit plus'then price_ttc else 0 end) - max( case when sale_type ='Abonnement' then subscription_price end  ) as petitplus_price,
from data_locker
--where sale_id = '62d83d77f1b8dbeedb99c42e'
group by 1,2,3,4,5,6,7,8,9
) , 

bonus_consolidation as (
select 
   sale_date as sale_date_bonus,
   sum(petitplus) as nb_bonus,
   round(sum(case when petitplus > 0 then petitplus_price end)/100/1.055,2) as bonus_revenue, 
  from bonus_data
  group by 1
) , 

final_order as (
select
    sale_date, 
    DATE_TRUNC(sale_date, WEEK(MONDAY)) as first_day_week,
    LAST_DAY(sale_date, WEEK(MONDAY)) as last_day_week,
    --add 1 to week number to agline with theory sale Kraken file       
    week_number + 1 as week_number, 
    month, 
    count(distinct case when sale_type ='Boutique' then sale_id end ) as nb_order, 
    count(distinct case when sale_type ='Abonnement' then sale_id end ) as nb_locker,
    count(distinct case when sale_type ='Boutique' then user_id end ) as nb_customer_order, 
    count(distinct case when sale_type ='Abonnement' then user_id end ) as nb_customer_locker,     
    round(sum( case when sale_type ='Boutique' then price_ttc*offerings_value_count end )/1.055,2) as order_revenue_ttc, 
    round(sum( case when sale_type ='Abonnement' then price_ttc end )/1.055,2) as locker_revenue_ttc,
    round(sum( case when sale_type ='Boutique' then price_ht end ),2) as order_revenue_ht, 
    round(sum( case when sale_type ='Abonnement' then price_ht end ),2) as locker_revenue_ht,
    round(sum( case when sale_type ='Boutique' then cogs end ),2) as order_cogs, 
    round(sum( case when sale_type ='Abonnement' then cogs end ),2) as locker_cogs
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

order by final_order.sale_date asc  
















     

   