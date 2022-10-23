{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

-- Tunnel de conversion abonnement
select  
       PARSE_DATE('%Y%m%d',event_date) as Date, 
       device_category, 
       concat (PARSE_DATE('%Y%m%d',event_date),'_', device_category) as subscriptions_ligne_id, 
       --checkout_category, 
       count(distinct case when event_action='step0' and checkout_category = 'outsideFunnel' and event_label='subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step0,       
       count(distinct case when event_label='step1' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step1,
       count(distinct case when event_label='step2' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step2,
       count(distinct case when event_label='step3' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step3,
       count(distinct case when event_label='step4' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step4,
       count(distinct case when event_label='step5' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step5,
       count(distinct case when event_label='step6' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step6,
       count(distinct case when event_label='step8' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step8,
       count(distinct case when event_label='step9' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step9,
       count(distinct case when event_label='step10' and checkout_category = 'subscriptionFunnel' then user_pseudo_id end ) as subscriptionFunnel_step10,

from {{ ref('stg_ga_tunnel_conversion') }}
group by 1,2,3
order by date desc 