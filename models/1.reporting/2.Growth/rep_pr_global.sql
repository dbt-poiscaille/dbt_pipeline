{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
  place_id,
  place_name,
  place_description,
  postal_code,
  region_code_postal,
  nom_region,
  round(sum(charges_amount)/100,2) as amount, 
  count(distinct stripe_customer_id) as customers
 from {{ ref('stg_charges_consolidation') }}
  where place_id is not null 
  group by 1,2,3,4,5,6