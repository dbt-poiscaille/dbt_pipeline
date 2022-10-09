{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select  
       PARSE_DATE('%Y%m%d',event_date) as lockerFunnel_date, 
       device_category as lockerFunnel_device_category, 
       concat (PARSE_DATE('%Y%m%d',event_date),'_', device_category) as locker_ligne_id, 
       --checkout_category, 
       count(distinct case when event_label='step1' then user_pseudo_id end ) as locker_step1,
       count(distinct case when event_label='step2' then user_pseudo_id end ) as locker_step2,
       count(distinct case when event_label='step3' then user_pseudo_id end ) as locker_step3

from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'lockerFunnel'
group by 1,2,3
order by 1 desc 