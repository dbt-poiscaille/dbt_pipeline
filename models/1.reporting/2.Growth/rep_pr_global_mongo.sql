{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with 
  place_consolidation as (
    select
      place_id, 
      COUNT (DISTINCT(case when type_sale = 'Abonnement' then user_id end)) AS nb_subscribers,
      round(sum(case when type_sale = 'Abonnement' then sale_locker_ttc end ),2) as total_ca_subscriptions,
      round(sum(case when type_sale = 'Boutique' then sale_boutique_ttc  end ),2) as total_ca_shop,
      round(sum(case when type_sale = 'Petit plus' then sale_bonus_ttc end ),2) as total_ca_petitplus,
      COUNT (DISTINCT(case when type_sale = 'Petit plus' then sale_id end)) AS nb_vente_petitplus,
      COUNT (DISTINCT(case when type_sale = 'Petit plus' or type_sale = 'Boutique'  then sale_id end)) AS nb_vente_hors_abo,
      round(SUM(sale_total_ttc),2) AS total_ca , 
      round((SUM(sale_total_ttc)/count(distinct sale_id)),2) as pan_moy  , 
      round((sum(case when type_sale = 'Boutique' or type_sale = 'Petit plus' then (IFNULL(sale_locker_ttc,0) + IFNULL(sale_bonus_ttc,0)) end )/count( distinct case when type_sale = 'Boutique' or type_sale = 'Petit plus' then sale_id end)),2) as panier_moyen_hors_casier

    from {{ ref('stg_mongo_sale_consolidation') }} 
    where place_id IS NOT NULL
    and place_name <> 'Livraison à domicile'
    group by 1
  ),  

  sale_data as  (
    select 
      user_id as user_id_sale_data, 
      email as user_email,
      max(place_name) as place_name, 
      max(place_id) as place_id, 
    FROM {{ ref('stg_mongo_sale_consolidation') }}
    group by 1,2 ) ,


  users_discount as (
  SELECT
    distinct 
    id as discount_user_id,
    email as discount_user_email ,
    discount_coupon_id,
    discount_coupon_name,
    discount_coupon_percentoff,
  FROM {{ ref('src_stripe_customers') }}
  WHERE discount_coupon_id = 'uUm1gzIT'
  order by id asc ) , 


  discount_consolidation as (
  select 
    user_id_sale_data, 
    user_email,
    place_name, 
    place_id, 
    users_discount.discount_coupon_id,
    users_discount.discount_user_email,
    users_discount.discount_coupon_name
  from sale_data
  left join users_discount
  on sale_data.user_email = users_discount.discount_user_email
  where discount_user_email is not null 
  order by users_discount.discount_coupon_id desc
  ),

  place_discount as (
    select 
      place_id as discount_place_id,
      place_name as discount_place_name, 
      count(distinct discount_user_email) as nombre_50_per_cent
    from discount_consolidation
    group by 1,2
  ), 

  place_info as (
  select distinct 
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
  ),

  place_data_src as (
    select distinct
      pr_email,
      pr_statut_lead,
      hs_lead_status
    from {{ ref('stg_mongo_vs_hubspot') }}
  )

select 
   place_consolidation.*,
  'Utilisateur B2B' as contact_type,
  nombre_50_per_cent,
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
  place_type,
  pr_statut_lead,
  hs_lead_status
from place_consolidation
left join place_info
on place_consolidation.place_id = place_info.place_id 
left join place_discount
on place_consolidation.place_id = place_discount.discount_place_id
left join place_data_src
on place_data_src.pr_email = place_info.place_email
order by place_consolidation.place_id asc 
  



