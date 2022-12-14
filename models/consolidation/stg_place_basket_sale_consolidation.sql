{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
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
  --round(cast(price_ttc as int64)/100,2) as price_ttc,
  round(cast(offerings_value_price_ttc as int64)/100,2) as price_ttc,  
  refundedprice /100 as amount_refund,
  customerid,
  subscriptionid, 
  subscription_rate,
  case when subscription_rate = 'biweekly' then 'Livraison chaque quinzaine'
       when subscription_rate = 'weekly' then 'Livraison chaque semaine'
       when subscription_rate = 'fourweekly' then 'Livraison chaque mois'
       end as subscription_type,   
  subscription_total_casiers,
  channel,
  offerings_value_channel,
  type_sale,  
  round(cast(offerings_value_price_ttc as int64)/100,2) as price_details_ttc,
  offerings_value_price_tax,
  offerings_value_price_ht,
  subscription_price,  
  offerings_value_name,
  --offerings_value_items_value_product_name,
  --offerings_value_items_value_product_id,
  --offerings_value_items_value_product_type,
  invoiceitemid,
  chargeid,
  status
  FROM  {{ ref('src_mongodb_sale') }} 
  order by subscription_total_casiers asc 
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
FROM {{ ref('stg_mongo_place_consolidation') }}
),

mongo_sale_basket_consolidation as (
  SELECT
    distinct
    sale_data.*,
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
  FROM sale_data LEFT JOIN place_data ON sale_data.place_id = place_data.place_id
  order by sale_date desc ,  sale_id asc 
)

SELECT
  sale_date,
  firstname,
  lastname,
  phone,
  user_id,
  email,
  place_id, 
  company, 
  place_name,
  place_owner, 
  place_city, 
  place_address, 
  place_codepostal, 
  place_email,
  place_createdat, 
  shipping_pickup, 
  shipping_delay, 
  shipping_company, 
  nom_departement, 
  nom_region,
  type_sale,
  subscription_type,
  type_livraison,
  offerings_value_name,
  CASE 
    WHEN offerings_value_name LIKE 'Casier%' THEN SPLIT(offerings_value_name)[SAFE_OFFSET(0)]
    ELSE NULL
  END AS basket_type,
  price_details_ttc
  -- COUNT(distinct sale_id) AS total_number_basket,
  -- COUNT(DISTINCT CASE WHEN subscription_type IS NOT NULL THEN sale_id ELSE NULL END) AS number_baskets_subscription,
  -- COUNT(DISTINCT CASE WHEN subscription_type IS NULL THEN sale_id ELSE NULL END) AS number_baskets_nosubscription,
  -- ROUND(SUM(price_details_ttc),2) AS total_sale_amount,
  -- ROUND(SUM(CASE WHEN subscription_type IS NOT NULL THEN price_details_ttc ELSE 0 END),2) AS sale_amount_subscription,
  -- ROUND(SUM(CASE WHEN subscription_type IS NULL THEN price_details_ttc ELSE 0 END),2) AS sale_amount_nosubscription,
  -- ROUND(SUM(price_details_ttc)/COUNT(DISTINCT sale_id),2) AS average_sale_amount_per_basket
FROM mongo_sale_basket_consolidation
-- WHERE user_id = '61c0cbd4b0e2abe12d8d8961'
-- GROUP BY
--   sale_date,
--   firstname,
--   lastname,
--   phone,
--   user_id,
--   email,
--   place_id, 
--   company, 
--   place_name,
--   place_owner, 
--   place_city, 
--   place_address, 
--   place_codepostal, 
--   place_email, 
--   place_createdat, 
--   shipping_pickup, 
--   shipping_delay, 
--   shipping_company, 
--   nom_departement, 
--   nom_region,
--   type_sale,
--   subscription_type,
--   type_livraison,
--   offerings_value_name,
--   basket_type
ORDER BY place_name ASC, sale_date DESC