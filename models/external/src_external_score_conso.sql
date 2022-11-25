{{
  config(
    materialized = 'table',
    labels = {'type': 'external', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select distinct
  produit as product,
  type,
  cast(replace(code, ',', '.') as float64) as score
from {{ source('external', 'data_score_conso') }}
where produit is not null