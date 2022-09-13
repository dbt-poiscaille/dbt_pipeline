{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
   distinct 
  _id,
  type as type,
  latinname,
  faocode,
  name,
  --createdat,
  --updatedat,
  allergens.value as allergens
  -- aller chercher image produit dans la base offering 
  -- aller chercher le prix dans la base offering 
  
from {{ source('mongodb', 'product') }}, 
  unnest (allergens) allergens
order by _id asc 