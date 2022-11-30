{{
  config(
    materialized = 'table',
    labels = {'type': 'external', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select
  *
from {{ source('external', 'code_promo_dictionary') }}