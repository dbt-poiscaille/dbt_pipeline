

{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select 
  place_id,
  place_name,
  place_owner,
  place_address,
  place_codepostal,
  code_postal,
  place_lng,
  place_lat,
  place_city,
  place_geocode,
  place_createdat,
  days_since_in_bdd,
  months_since_in_bdd,
  year_since_in_bdd,
  place_storage,
  place_icebox,
  place_phone,
  place_pickup,
  place_company,
  place_openings_schedule,
  place_openings_hidden,
  place_openings_depositschedule,
  nom_departement,
  nom_region

  from {{ ref('stg_place_consolidation_region') }}