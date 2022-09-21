{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
       PARSE_DATE('%Y%m%d', event_date) AS event_date,
      count( distinct user_pseudo_id) as users 
 from {{ ref('scr_ga_global_data') }}
 group by 1 
 order by 1 desc 