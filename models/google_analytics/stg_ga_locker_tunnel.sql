{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select  
       PARSE_DATE('%Y%m%d',event_date) as Date, 
       device_category, 
       checkout_category, 
       count(distinct case when event_label='step1' then user_pseudo_id end ) as step1,
       count(distinct case when event_label='step2' then user_pseudo_id end ) as step2,
       count(distinct case when event_label='step3' then user_pseudo_id end ) as step3

from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'lockerFunnel'
group by 1,2,3
order by date desc 