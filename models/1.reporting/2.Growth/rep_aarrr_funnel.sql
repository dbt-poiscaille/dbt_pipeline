
{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}
with 
shop_funnel as (
select
       Date_shop_funnel, 
       device_category, 
       shop_ligne_id, 
       shopFunnel_step0,
       shopFunnel_step1,
       shopFunnel_step2,
       shopFunnel_step3,
       shopFunnel_step4,
       shopFunnel_step5,
       shopFunnel_step6,
       shopFunnel_step7
from {{ ref('stg_ga_boutique_tunnel') }}
) ,

subscription_funnel as (
select 
       Date, 
       subscriptions_ligne_id, 
       subscriptionFunnel_step0,
       subscriptionFunnel_step1,
       subscriptionFunnel_step2,
       subscriptionFunnel_step3,
       subscriptionFunnel_step4,
       subscriptionFunnel_step5,
       subscriptionFunnel_step6,
       subscriptionFunnel_step8,
       subscriptionFunnel_step9,
       subscriptionFunnel_step10
from {{ ref('stg_ga_subscriptions_tunnel') }}
) , 

locker_funnel as  (
  select 
       lockerFunnel_date, 
       locker_ligne_id, 
       locker_step1,
       locker_step2,
       locker_step3
from {{ ref('stg_ga_locker_tunnel') }}
) , 

ptitplus_funnel as (
select  
       plusFunnel_Date, 
       plus_ligne_id, 
       plusFunnel_step4,
       plusFunnel_step5,
       plusFunnel_step6

from {{ ref('stg_ga_petitplus_tunnel') }}
)

select 
 * 
 from shop_funnel 
 left join subscription_funnel
 on shop_funnel.shop_ligne_id = subscription_funnel.subscriptions_ligne_id
 left join locker_funnel 
 on shop_funnel.shop_ligne_id = locker_funnel.locker_ligne_id
 left join ptitplus_funnel
 on shop_funnel.shop_ligne_id = ptitplus_funnel.plus_ligne_id
