{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with place_data as (
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
 from {{ ref('stg_place_final') }}
), 

place_agg as (
select
  pr_id,
  pr_name,
  pr_adresse,
  pr_code_postal,
  --nom_region,
  round(sum(charges_amount)/100,2) as amount, 
  count(distinct stripe_customer_id) as customers
 from {{ ref('stg_charges_consolidation') }}
  where pr_id is not null 
  group by 1,2,3,4
)

select 
 *
 from place_data
 left join place_agg 
 on place_data.place_id = place_agg.pr_id