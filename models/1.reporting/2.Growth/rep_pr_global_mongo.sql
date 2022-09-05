{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

WITH place_data AS (
  SELECT place_id,
        place_name, 
        place_city , 
        place_address,
        CAST(place_createdat AS DATE) AS DATE_CREATION
        FROM {{ ref('stg_mongo_place_consolidation') }} 
),
sale_data AS (
SELECT 
place_id,
place_name, 
place_city , 
place_address, 
CAST(place_createdat AS DATE) AS DATE_CREATION,
COUNT (DISTINCT(case when type_sale = 'abonnement' then user_id end)) AS nb_subscribers,
sum(case when type_sale = 'abonnement' then price_ttc end ) as total_subscriptions,
sum(case when type_sale = 'shop' then price_ttc end ) as total_shop,
sum(case when type_sale = 'Petit plus' then price_ttc end ) as total_petitplus,
COUNT (DISTINCT(case when type_sale = 'Petit plus' then sale_id end)) AS nb_vente_petitplus,
COUNT (DISTINCT(case when type_sale = 'Petit plus' or type_sale = 'shop'  then sale_id end)) AS nb_vente_hors_abo,
SUM(price_ttc) AS CA 
FROM {{ ref('stg_mongo_sale_consolidation') }} 
WHERE place_id IS NOT NULL
GROUP BY place_id,
place_name, 
place_city , 
place_address,
DATE_CREATION
)
SELECT place_data.place_id,
        place_data.place_name, 
        place_data.place_city , 
        place_data.place_address,
        place_data.DATE_CREATION,
        nb_subscribers,
        total_subscriptions,
        total_shop,
        total_petitplus,
        nb_vente_petitplus,
        nb_vente_hors_abo,
        CA
        FROM place_data
        LEFT JOIN sale_data ON place_data.place_id = sale_data.place_id

