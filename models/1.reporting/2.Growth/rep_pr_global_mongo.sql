{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with place_consolidation as (
select
place_id, 
COUNT (DISTINCT(case when type_sale = 'abonnement' then user_id end)) AS nb_subscribers,
round(sum(case when type_sale = 'abonnement' then price_details_ttc end )/100,2) as total_ca_subscriptions,
round(sum(case when type_sale = 'shop' then price_details_ttc  end )/100,2) as total_ca_shop,
round(sum(case when type_sale = 'Petit plus' then price_details_ttc end )/100,2) as total_ca_petitplus,
COUNT (DISTINCT(case when type_sale = 'Petit plus' then sale_id end)) AS nb_vente_petitplus,
COUNT (DISTINCT(case when type_sale = 'Petit plus' or type_sale = 'shop'  then sale_id end)) AS nb_vente_hors_abo,
round(SUM(price_details_ttc)/100,2) AS total_ca , 
round((SUM(price_details_ttc)/count(distinct sale_id))/100,2) as pan_moy  , 
round((sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_details_ttc end )/count( distinct case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end))/100,2) as panier_moyen_hors_casier

-- montant des remboursements
-- Panier Moyen hors ca
FROM {{ ref('stg_mongo_sale_consolidation') }} 
WHERE place_id IS NOT NULL
GROUP BY 1
) ,  
place_info as (

select 
distinct 
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
  zone, 
  case when nom_region = 'Île-de-France' then 'IDF' else 'Région' end as place_location,
  'France' as place_country, 
  'PR' as place_type
  FROM {{ ref('stg_mongo_sale_consolidation') }} 

)

select 
   place_consolidation.*,
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
  zone,
  place_location,
  place_country,
  place_type
  from place_consolidation
  left join place_info
  on place_consolidation.place_id = place_info.place_id 
  order by place_consolidation.place_id asc 
  



