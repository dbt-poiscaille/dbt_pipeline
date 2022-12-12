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
       traffic_name, 
       traffic_source, 
       traffic_medium,  
       --checkout_category, 
       count(distinct case when event_action='step0' and checkout_category = 'outsideFunnel' and event_label='shopFunnel' then user_pseudo_id end ) as shopFunnel_step0,       
       count(distinct case when event_label='step1' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step1,
       count(distinct case when event_label='step2' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step2,
       count(distinct case when event_label='step3' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step3,
       count(distinct case when event_label='step4' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step4,
       count(distinct case when event_label='step5' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step5,
       count(distinct case when event_label='step6' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step6,
       count(distinct case when event_label='step7' and checkout_category = 'shopFunnel' then user_pseudo_id end ) as shopFunnel_step7

from {{ ref('stg_ga_tunnel_conversion') }}
group by 1,2,3,4,5,6
order by 1 desc 