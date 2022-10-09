{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with mongo_sale_data as (
select
  distinct
  sale_id,
  place_id,
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

  margin,
  margin__fl,
  case 
      when margin is not null and margin__fl is null then margin 
      when margin__fl is not null and margin is null then margin__fl
      end as margin_final,  
  price_ttc, 
  price_ht, 
  offerings_value_count,
  --offerings_value_name,
  --offerings_value_items_value_portion_unit,
  --offerings_value_items_value_portion_quantity,
  --offerings_value_items_value_cost_ttc,
  --offerings_value_item_value_cost_ht,
  --offerings_value_items_value_cost_unit,
  --offerings_value_items_value_product_name,
  email,
  user_id,
  name
from
 {{ ref('src_mongodb_sale') }}
),


data_locker_bonus as (
  select *,
  CASE WHEN channel = 'shop' THEN 'Boutique'
    WHEN  channel = 'combo' and offerings_value_channel = 'combo' THEN 'Abonnement'
    WHEN  channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
  end as sale_type,
  case 
    when margin_final is null then null
    when margin_final is not null then ((100 - margin_final)/100) * (offerings_value_price_ttc/100/1.055)
    end as cogs
  from mongo_sale_data
),


place_data AS (
SELECT
  place_id,
  place_name,
  place_owner,
  place_phone,
  place_city,
  place_address,
  place_codepostal,
  place_email,
  place_coupon,
  place_lng,
  place_lat,
  place_geocode,
  place_createdat,
  shipping_pickup,
  shipping_delay,
  place_company,
  place_coupon_users,
  place_coupon_amount,
  shipping_company,
  days_since_in_bdd,
  months_since_in_bdd,
  year_since_in_bdd,
  type_livraison,
  place_storage,
  place_icebox,
  place_pickup,
  place_openings_schedule,
  place_openings_hidden,
  place_openings_day,
  place_openings_depositschedule,
  nom_departement,
  nom_region,
  zone
FROM  {{ ref('stg_mongo_place_consolidation') }}
),

sale_type_data as (
  select
    sale_id,
    place_id,
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
    max(case when sale_type ='Abonnement' then subscription_price end  ) as locker_price,
    max(case when sale_type ='Abonnement' then price_ttc end  ) as locker_price, 
    max(case when sale_type = 'Petit plus'then price_ttc else 0 end)  as price_ttc_bonus,
    sum(case when sale_type = 'Abonnement' then offerings_value_price_ttc end) as price_ttc_locker,
    max(case when sale_type = 'Petit plus'then price_ttc else 0 end) - max( case when sale_type ='Abonnement' then subscription_price end) as petitplus_price,
    sum(case when sale_type = 'Boutique' then offerings_value_price_ttc*offerings_value_count end) as order_sale,
    sum(case when sale_type = 'Abonnement' then cogs end) as locker_cog,
    sum(case when sale_type = 'Boutique' then cogs end) as order_cog
  from data_locker_bonus
  group by 1,2,3,4,5,6,7,8,9,10
),

sale_consolidation as (
  select
    sale_type_data.*,
    place_name,
    place_owner,
    place_phone,
    place_city,
    place_address,
    place_codepostal,
    place_email,
    place_coupon,
    -- place_lng,
    -- place_lat,
    -- place_geocode,
    -- place_createdat,
    shipping_pickup,
    shipping_delay,
    place_company,
    -- place_coupon_users,
    -- place_coupon_amount,
    shipping_company,
    days_since_in_bdd,
    months_since_in_bdd,
    year_since_in_bdd,
    type_livraison,
    -- place_storage,
    -- place_icebox,
    -- place_pickup,
    -- place_openings_schedule,
    -- place_openings_hidden,
    -- place_openings_day,
    -- place_openings_depositschedule,
    nom_departement,
    nom_region,
    zone

  from sale_type_data
  left join place_data
  on sale_type_data.place_id = place_data.place_id
),

coupon_refund as (
  select 
   in_date,
   nb_customer_cp,
   total_amount_cp,
   charge_date,
   amount_refunded
   from {{ ref('stg_coupon_refund_consolidation') }}
),

result as (
    select
        sale_date,
        DATE_TRUNC(sale_date, WEEK(MONDAY)) as first_day_week,
        LAST_DAY(sale_date, WEEK(MONDAY)) as last_day_week,
        --add 1 to week number to agline with theory sale Kraken file       
        week_number + 1 as week_number, 
        month,
        nom_departement,
        nom_region,
        zone,

        --Total
        sum(locker) as nb_locker,
        sum(petitplus) as nb_bonus,
        sum(case when locker = 0 and petitplus = 0 then 1 end) as nb_order,


        count(distinct case when locker = 1 then user_id end) as nb_customer_locker,
        count(distinct case when petitplus = 1 then user_id end) as nb_customer_bonus,
        count(distinct case when locker = 0 and petitplus = 0 then user_id end) as nb_customer_order,
        
        round(sum(case when locker = 1 then price_ttc_locker end)/100/1.055,2) as locker_revenue,
        round(sum(case when petitplus = 1 then petitplus_price end)/100/1.055,2) as bonus_revenue,
        round(sum(case when locker = 0 and petitplus = 0 then order_sale end)/100/1.055,2) as order_revenue,

        round(sum(locker_cog),2) as locker_cog,
        round(sum(order_cog),2) as order_cogs

    from sale_consolidation
    group by 1,2,3,4,5,6,7,8
)

select 
    *
from result
order by sale_date