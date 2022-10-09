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
        rank() over ( partition by user_id order by sale_date asc ) as rank
   from {{ ref('stg_mongo_sale_consolidation') }}
   where type_sale = 'Boutique'
   order by user_id asc , rank asc 
)

select 
     sale_date, 
     count( distinct user_id) as new_customers 
     from data_subscription
     where rank = 1
     group by 1
     order by sale_date desc 