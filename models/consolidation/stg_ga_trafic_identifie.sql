{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select 
  * from {{ ref('scr_ga_trafic_identifie') }}