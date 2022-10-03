{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select  
       PARSE_DATE('%Y%m%d',event_date) as Date, 
       device_category, 
       checkout_category, 
       count(distinct case when event_label='step4' then user_pseudo_id end ) as step4,
       count(distinct case when event_label='step5' then user_pseudo_id end ) as step5,
       count(distinct case when event_label='step6' then user_pseudo_id end ) as step6

from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'plusFunnel'
group by 1,2,3
order by date desc 