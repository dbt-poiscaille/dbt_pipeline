{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


WITH  sale_data_ttc_bonus as (
  select *
  from {{ ref('stg_mongo_sale_cleanup') }}
),

sale_data_w_prev_transaction as (
  select *,

  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date) as prev_sale_date,
  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date and s1.type_sale = 'Abonnement') as prev_subscription_sale_date,
  (select max(sale_date) from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date < s0.sale_date and s1.type_sale = 'Boutique') as prev_shop_sale_date,

  (select sum(sale_locker_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, sale_locker_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Abonnement')) as curr_total_subscription_revenue,
  (select sum(sale_boutique_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, sale_boutique_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Boutique')) as curr_total_shop_revenue,
  (select sum(sale_bonus_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, sale_bonus_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date and s1.type_sale = 'Petit plus')) as curr_total_bonus_revenue,
  (select sum(sale_total_ttc) from (select distinct sale_id, sale_date, user_id, type_sale, sale_total_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date)) as curr_total_revenue,
  (select round(sum(sale_total_ttc)/count(distinct sale_id),2) from (select distinct sale_id, sale_date, user_id, type_sale, sale_total_ttc from sale_data_ttc_bonus s1 where s1.user_id = s0.user_id and s1.sale_date <= s0.sale_date)) as curr_avg_renevue,

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

subscription_data as (
  select distinct
    user_type_user_id,
    startingat,
    user_status_
    from {{ ref('stg_users_subscription_type') }}
),

result as (
  SELECT sale_data_final.*, 
    place_name,
    place_owner,
    place_phone,
    place_city,
    place_address,
    place_codepostal,
    -- case 
    --   when place_name = 'Livraison à domicile' then shipping_city
    --   else place_city
    -- end as place_city,
    -- case 
    --   when place_name = 'Livraison à domicile' then shipping_addresse
    --   else place_address
    -- end as place_address,
    -- case 
    --   when place_name = 'Livraison à domicile' then shipping_codepostal
    --   else place_codepostal
    -- end as place_codepostal,
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
    case when nom_region is null then 'Livraison Domicile' else nom_region end as place_region,
    case when nom_region = 'Île-de-France' then 'IDF' else 'Region' end as region_type,
    zone,
    ifnull(user_status_, 'Sans Abonnement') as user_status_
  FROM sale_data_final 
    LEFT JOIN place_data ON sale_data_final.place_id = place_data.place_id
    LEFT JOIN subscription_data ON sale_data_final.user_id = subscription_data.user_type_user_id
  order by sale_date desc ,  sale_id asc 

)

select * from result
-- where sale_id = '632db1102fc2eea1097cbcae'
-- where user_id = '626984054b3baa57bb9f6744'
-- where user_transaction_phase = 'Méga-Abonné'
-- where customerid = 'cus_Fe5owEuY5Vh7Ko'
