

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
    concat(lat,',',lng) as place_geocode, 
    createdat as place_createdat, 
    updatedat as place_updatedat, 
    storage as place_storage, 
    icebox as place_icebox, 
    phone as place_phone, 
    pickup as place_pickup,
    company as place_company, 
    openings_schedule as place_openings_schedule, 
    openings_hidden as place_openings_hidden, 
    openings_day as place_openings_day, 
    openings_depositschedule as place_openings_depositschedule, 
    quantity as place_quantity, 
    quantity__fl as place_quantity__fl, 
    closing_raison as place_closing_raison , 
    closing_to as place_closing_to, 
    closing_from as place_closing_from, 
    shipping.pickup as shipping_pickup,
    shipping.delay as shipping_delay, 
    shipping.company as shipping_company
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
    FROM `poiscaille-358510.external.communes_departement_region`
 ),
 final_table AS 
 (
 SELECT
 distinct 
    place_id, 
    place_name, 
    place_owner, 
    TRIM( SPLIT( TRIM(SPLIT(place_address, ',')[SAFE_OFFSET(1)]), ' ')[SAFE_OFFSET(0)] )  as place_city , 
    place_address, 
    place_codepostal, 
    code_postal,
    place_lng, 
    place_lat , 
    place_geocode, 
    place_createdat, 
    shipping_pickup , 
    shipping_delay , 
    shipping_company , 
    date_diff(current_date(), cast(place_createdat as date), day) as days_since_in_bdd,
    date_diff(current_date(), cast(place_createdat as date), month) as months_since_in_bdd,
    date_diff(current_date(), cast(place_createdat as date ), year) as year_since_in_bdd,
    CASE WHEN place_name LIKE '%Livraison%' THEN 'Domicile' ELSE 'PR' END AS type_livraison,
    --place_updatedat, 
    place_storage, 
    place_icebox, 
    place_phone, 
    place_pickup,
    place_company, 
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
    --nom_commune_postal, 
    row_number() over(partition by place_id order by place_name desc) as rn 
    from data_place
    left join communes_departement_region
    on data_place.place_codepostal = communes_departement_region.code_postal
    order by place_name asc 
 )
 select *  except(rn) from final_table 
where rn = 1
order by place_id asc 
