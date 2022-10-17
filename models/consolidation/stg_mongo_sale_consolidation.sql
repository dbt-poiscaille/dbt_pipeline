{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


WITH  sale_data AS (
select  
 distinct 
  shippingat,
  DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
  DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
  LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,        
  sale_id,
  place_id, 
  company, 
  firstname,
  lastname,
  phone,
  user_id,
  email,
  createdat,
  subscription_id,
  price_ttc as price_ttc_raw,
  --round(cast(price_ttc as int64)/100,2) as price_ttc,
  -- round(cast(offerings_value_price_ttc as int64)/100,2) as price_ttc,  
  refundedprice /100 as amount_refund,
  customerid,
  subscriptionid, 
  subscription_rate,
  subscription_status,
  case when subscription_rate = 'biweekly' then 'Livraison chaque quinzaine'
       when subscription_rate = 'weekly' then 'Livraison chaque semaine'
       when subscription_rate = 'fourweekly' then 'Livraison chaque mois'
       end as subscription_type,   
  subscription_total_casiers,
  channel,
  offerings_value_channel,
  CASE WHEN channel = 'shop' THEN 'Boutique'
      WHEN  channel = 'combo' and offerings_value_channel = 'combo' THEN 'Abonnement'
      WHEN  channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
  END AS type_sale,  
  round(cast(offerings_value_price_ttc as int64)/100,2) as price_details_ttc,
  offerings_value_price_ttc,
  offerings_value_price_tax,
  offerings_value_price_ht,
  subscription_price,
  offerings_value_count,
  --offerings_value_name,
  --offerings_value_items_value_product_name,
  --offerings_value_items_value_product_id,
  --offerings_value_items_value_product_type,
  invoiceitemid,
  chargeid,
  status, 
  1*offerings_value_count as sales_count, 
  FROM  {{ ref('src_mongodb_sale') }} 
  order by subscription_total_casiers asc 
),

sale_data_ttc_bonus as (
  select
    *,
  case
    when type_sale = 'Boutique' then round(cast(offerings_value_price_ttc*offerings_value_count as int64)/100,2)
    when type_sale = 'Abonnement' then round(cast(subscription_price as int64)*offerings_value_count/100,2) 
    when type_sale = 'Petit plus' then round(cast(price_ttc_raw as int64)  - cast(subscription_price as int64)*offerings_value_count /100,2)  
  end as price_ttc
  from sale_data
),

sale_data_w_prev_transaction as (
  select *,

  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date) as prev_sale_date,
  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date and s1.type_sale = 'Abonnement') as prev_subscription_sale_date,
  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date and s1.type_sale = 'Boutique') as prev_shop_sale_date,

  (select sum(price_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Abonnement')) as curr_total_subscription_revenue,
  (select sum(price_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Boutique')) as curr_total_shop_revenue,
  (select sum(price_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date)) as curr_total_revenue,
  (select round(sum(price_ttc)/count(distinct sale_id),2) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date)) as curr_avg_renevue,

  (select count(distinct sale_id) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date)) as curr_nb_transaction,
  (select count(distinct sale_id) from (select distinct sale_id, sale_date, user_id, type_sale, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Abonnement')) as curr_nb_transaction_locker,
  (select sum(subscription_total_casiers) from (select distinct sale_id, sale_date, user_id, type_sale, subscription_total_casiers, price_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Abonnement')) as curr_nb_total_locker,

  case 
    when (select count(distinct subscriptionid) from (select distinct sale_id, sale_date, user_id, type_sale, subscriptionid from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date)) > 0 
    and subscriptionid is null
  then 'True'
  else 'False'
  end as curr_unsubcribed 
  
  from sale_data_ttc_bonus s0
),

sale_data_final as (
  select *,
  case
    when curr_nb_transaction = 1
      and (subscription_total_casiers is null or (subscription_total_casiers = 1 and subscriptionid is not null))
      and (date_diff(current_date(),prev_sale_date,day) < 90 or prev_sale_date is null)
    then 'Première transaction'
    when curr_nb_transaction > 1
      and curr_total_shop_revenue > 0
      and date_diff(current_date(),prev_sale_date,day) < 90
      and (curr_total_subscription_revenue = 0 or curr_unsubcribed = 'True')
    then 'Client boutique'
    when curr_nb_transaction > 1
      and curr_total_subscription_revenue > 0
      and subscriptionid is not null
    then 'Abonné'
    when curr_nb_transaction > 1
      and round(curr_total_revenue/100/1.055,2) > 2000
      and subscriptionid is not null
      and (
        curr_nb_total_locker > 25
        or round(curr_avg_renevue/100/1.055) > 100
      )
      -- & nb godson
    then 'Client promoteur'
    when curr_nb_transaction > 1
      and round(curr_total_revenue/100/1.055,2) > 4000
      -- and remboursement < 200
      and subscriptionid is not null
    then 'Méga-Abonné'
    when (
      curr_unsubcribed = 'True'
        and (
          (
            -- no store buy after unsubscribe
            select count(distinct sale_id) 
            from (
              select distinct sale_id, sale_date, user_id, type_sale 
              from sale_data_w_prev_transaction s1 
              where s1.user_id = s0.user_id 
                and s1.sale_date <= s0.sale_date
                and s1.sale_date > s0.prev_subscription_sale_date
                and s1.type_sale = 'Boutique'
            )
          ) = 0
          -- or no store buy after unsubcrube in the last 3 months
          or (
            select count(distinct sale_id) 
            from (
              select distinct sale_id, sale_date, user_id, type_sale 
              from sale_data_w_prev_transaction s1 
              where s1.user_id = s0.user_id 
                and s1.sale_date <= s0.sale_date
                and s1.sale_date > s0.prev_subscription_sale_date
                and s1.type_sale = 'Boutique'
                and date_diff(current_date(), prev_shop_sale_date, day) < 90
            )
          ) = 0
        )
    )

    or (
      curr_total_revenue = 0
      and curr_total_shop_revenue = 0
      and date_diff(current_date(), prev_sale_date, day) > 90 
    )
    then 'Ancien client'

  else 'Autre'
  end as user_transaction_phase 

  from sale_data_w_prev_transaction s0
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
FROM  {{ ref('stg_mongo_place_consolidation') }}),

result as (
  SELECT sale_data_final.*, 
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
  FROM sale_data_final 
    LEFT JOIN place_data ON sale_data_final.place_id = place_data.place_id
  order by sale_date desc ,  sale_id asc 

)

select * from result
-- where sale_id = '62cc5b3a9a26adf00ba40d58'
-- where user_id = '626984054b3baa57bb9f6744'
-- where user_transaction_phase = 'Méga-Abonné'
-- where customerid = 'cus_Fe5owEuY5Vh7Ko'
