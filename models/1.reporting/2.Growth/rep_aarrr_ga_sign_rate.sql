{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select 
  event_date,
  device_category,
  traffic_name,
  traffic_medium,
  traffic_source,
  nb_utilisateurs,
  total_signIn,
  total_signUp
from {{ ref('scr_ga_global_events') }}