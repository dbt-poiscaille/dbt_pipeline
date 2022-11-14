{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
    *
from {{ source('hubspot', 'companies') }}