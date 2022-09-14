



{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


select
      event_date, 
      count( distinct user_pseudo_id) as users 
 from {{ ref('scr_ga_global_data') }}
 group by 1 
 order by 1