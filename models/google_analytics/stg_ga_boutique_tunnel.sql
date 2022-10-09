{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select  
       PARSE_DATE('%Y%m%d',event_date) as Date_shop_funnel, 
       device_category, 
       concat (PARSE_DATE('%Y%m%d',event_date),'_', device_category) as shop_ligne_id, 
       --checkout_category, 
       count(distinct case when event_label='step1' then user_pseudo_id end ) as shopFunnel_step1,
       count(distinct case when event_label='step2' then user_pseudo_id end ) as shopFunnel_step2,
       count(distinct case when event_label='step3' then user_pseudo_id end ) as shopFunnel_step3,
       count(distinct case when event_label='step4' then user_pseudo_id end ) as shopFunnel_step4,
       count(distinct case when event_label='step5' then user_pseudo_id end ) as shopFunnel_step5,
       count(distinct case when event_label='step6' then user_pseudo_id end ) as shopFunnel_step6,
       count(distinct case when event_label='step7' then user_pseudo_id end ) as shopFunnel_step7

from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'shopFunnel'
group by 1,2,3
order by 1 desc 