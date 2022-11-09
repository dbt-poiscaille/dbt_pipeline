{{
  config(
    materialized = 'table',
    labels = {'type': 'external', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select
  *
from {{ source('external', 'nomenclature_cp_final') }}