{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_place as (
select 
    _id as place_id, 
    name as place_name, 
    details as place_owner, 
    description as place_address, 
    postalcode as place_codepostal, 
    lng as place_lng, 
    lat as place_lat , 
    email as place_email, 
    phone as place_phone, 
    concat(lat,',',lng) as place_geocode, 
    createdat as place_createdat, 
    updatedat as place_updatedat, 
    storage as place_storage, 
    icebox as place_icebox, 
    pickup as place_pickup,
    company as place_company, 
    openings_day_fr as place_openings_day,
    openings_hidden as place_openings_hidden,    
    openings_depositschedule as place_openings_depositschedule, 
    quantity as place_quantity, 
    openings_schedule as place_openings_schedule,
    quantity__fl as place_quantity__fl, 
    closing_raison as place_closing_raison , 
    closing_to as place_closing_to, 
    closing_from as place_closing_from, 
    shipping.pickup as shipping_pickup,
    shipping.delay as shipping_delay, 
    shipping.company as shipping_company,
    comment
 from {{ ref('src_mongodb_place') }}
) , 
 communes_departement_region AS 
 (
    SELECT 
    code_postal,	
    latitude,
    longitude,
    code_commune,
    nom_commune,
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    zone

    FROM `poiscaille-358510.external.communes_departement_region`
 ),

 code_promo as ( 
SELECT
  ID as pr_id,
  CP as pr_cp,
  PR as pr
FROM
  `poiscaille-358510.external.pr_code_promo`
 ),

 code_promo_perf as (

    select 
          trim(replace(cp_name, 'Coupon','')) as cp_name, 
          nb_customer, 
          total_amount
     from {{ ref('stg_coupons_consolidation_place') }}
 ), 

 final_table AS 
 (
 select
 distinct 
    place_id, 
    place_name, 
    place_owner, 
    concat('+33',substr(place_phone,2,9)) as place_phone,
    TRIM( SPLIT( TRIM(SPLIT(place_address, ',')[SAFE_OFFSET(1)]), ' ')[SAFE_OFFSET(0)] )  as place_city , 
    place_address, 
    place_codepostal, 
    place_email, 
    pr_cp as place_coupon , 
    place_lng, 
    place_lat , 
    place_geocode,     
    place_createdat, 
    shipping_pickup , 
    shipping_delay , 
    place_company,
    comment,
    code_promo_perf.nb_customer as place_coupon_users,
    code_promo_perf.total_amount as place_coupon_amount,
    shipping_company , 
    date_diff(current_date(), cast(place_createdat as date), day) as days_since_in_bdd,
    date_diff(current_date(), cast(place_createdat as date), month) as months_since_in_bdd,
    date_diff(current_date(), cast(place_createdat as date ), year) as year_since_in_bdd,
    CASE WHEN place_name LIKE '%Livraison%' THEN 'Domicile' ELSE 'PR' END AS type_livraison,
    --place_updatedat, 
    place_storage, 
    place_icebox, 
    place_pickup,
    place_openings_schedule, 
    place_openings_hidden, 
    place_openings_day, 
    place_openings_depositschedule, 
    --place_quantity, 
    --place_quantity__fl, 
    --place_closing_raison , 
    --place_closing_to, 
    --place_closing_from,
    nom_departement, 
    nom_region, 
    zone,
    row_number() over(partition by place_id order by place_name desc) as rn 
    from data_place
    left join communes_departement_region
    on data_place.place_codepostal = communes_departement_region.code_postal
    left join code_promo 
    on data_place.place_id = code_promo.pr_id
    left join code_promo_perf
    on code_promo.pr_cp = code_promo_perf.cp_name
    order by place_name asc 
 )
 select *  except(rn) from final_table 
where rn = 1
order by place_id asc 