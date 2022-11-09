{{
  config(
    materialized = 'table',
    labels = {'type': 'google_ads', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select 
  *
from {{ source('google_ads', 'campaign_performance_report') }}
