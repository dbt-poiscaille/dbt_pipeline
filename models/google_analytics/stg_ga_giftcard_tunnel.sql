{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select  
       PARSE_DATE('%Y%m%d',event_date) as plusFunnel_Date, 
       concat (PARSE_DATE('%Y%m%d',event_date),'_', device_category) as plus_ligne_id, 
       device_category, 
       traffic_name, 
       traffic_source, 
       traffic_medium,       
       ---checkout_category, 
       count(distinct case when event_label='step1' then user_pseudo_id end ) as giftcardFunnel_step1,
       count(distinct case when event_label='step2' then user_pseudo_id end ) as giftcardFunnel_step2,
       count(distinct case when event_label='step3' then user_pseudo_id end ) as giftcardFunnel_step3,
       count(distinct case when event_label='step4' then user_pseudo_id end ) as giftcardFunnel_step4, 
       count(distinct case when event_label='step5' then user_pseudo_id end ) as giftcardFunnel_step5


from {{ ref('stg_ga_tunnel_conversion') }}
where checkout_category = 'giftcardFunnel'
group by 1,2,3,4,5,6