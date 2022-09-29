{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

with data_subscription as (
select 
   distinct 
        sale_date , 
        sale_id, 
        user_id, 
        type_sale,
        subscription_id,
        subscription_rate,
        case when subscription_rate = 'biweekly' then 'Livraison chaque quinzaine'
       when subscription_rate = 'weekly' then 'Livraison chaque semaine'
       when subscription_rate = 'fourweekly' then 'Livraison chaque mois'     
       end as subscription_type,   
        subscriptionid, 
        rank() over ( partition by user_id order by sale_date asc ) as rank
   from {{ ref('stg_mongo_sale_consolidation') }}
   where type_sale = 'abonnement'
   order by user_id asc , rank asc 
) , 
consolidation_subscriptions as (
   select 
     sale_date, 
     count(distinct user_id) as total_new_subscribers,
     count( distinct case when subscription_rate = 'biweekly' then user_id end ) as new_livraison_chaque_quinzaine,
     count( distinct case when subscription_rate = 'weekly' then user_id end ) as new_livraison_chaque_semaine,
     count( distinct case when subscription_rate = 'fourweekly' then user_id end ) as new_livraison_chaque_mois, 
     from data_subscription
     where rank = 1 and subscription_type is not null
     group by 1
     order by sale_date desc 
), 

data_shop as (
select 
   distinct 
        sale_date , 
        sale_id, 
        user_id, 
        type_sale,
        rank() over ( partition by user_id order by sale_date asc ) as rank
   from {{ ref('stg_mongo_sale_consolidation') }}
   where type_sale = 'shop'
   order by user_id asc , rank asc 
), 

 consolidation_shop as ( 
select 
     sale_date, 
     count( distinct user_id) as new_customers 
     from data_shop
     where rank = 1
     group by 1
     order by sale_date desc 
 )

  select 
       consolidation_subscriptions.sale_date, 
       consolidation_subscriptions.total_new_subscribers, 
       consolidation_subscriptions.new_livraison_chaque_semaine,
       consolidation_subscriptions.new_livraison_chaque_quinzaine,
       consolidation_subscriptions.new_livraison_chaque_mois,
       consolidation_shop.new_customers as shop_new_customers

       from consolidation_subscriptions
       left join consolidation_shop
       on consolidation_subscriptions.sale_date = consolidation_shop.sale_date

