{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


WITH  sale_data AS (
SELECT 
  sale_id,
  channel,
  place_id, 
  company, 
  firstname,
  lastname,
  phone,
  user_id,
  email,
  createdat,
  subscription_id,
  round(cast(price_ttc as int64)/100,2) as price_ttc,
  refundedprice,
  customerid,
  subscriptionid, 
  subscription_total_casiers,
  offerings_value_name,
  offerings_value_items_value_product_name,
  offerings_value_items_value_product_id,
  offerings_value_items_value_product_type,
  CASE WHEN channel = 'shop' THEN 'shop'
      WHEN channel = 'combo' and offerings_value_channel = 'combo' THEN 'abonnement'
      WHEN channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
  END AS type_sale
  
  FROM  {{ ref('src_mongodb_sale') }} ),
  
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
FROM  {{ ref('stg_mongo_place_consolidation') }})
SELECT sale_data.*, 
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




