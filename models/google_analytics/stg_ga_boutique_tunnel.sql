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
       count(distinct case when event_label='step3' then user_pseudo_id end ) as step3,
       count(distinct case when event_label='step4' then user_pseudo_id end ) as step4,
       count(distinct case when event_label='step5' then user_pseudo_id end ) as step5,
       count(distinct case when event_label='step6' then user_pseudo_id end ) as step6,
       count(distinct case when event_label='step8' then user_pseudo_id end ) as step8,
       count(distinct case when event_label='step9' then user_pseudo_id end ) as step9,
       count(distinct case when event_label='step10' then user_pseudo_id end ) as step10,

from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'shopFunnel'
group by 1,2,3
order by date desc 