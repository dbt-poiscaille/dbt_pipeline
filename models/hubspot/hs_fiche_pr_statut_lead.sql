{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select distinct * from {{ ref('stg_hubspot_companies') }}