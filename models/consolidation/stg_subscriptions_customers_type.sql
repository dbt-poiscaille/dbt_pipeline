{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_info as (
SELECT
  id as subscription_id,
  user_id,
  formula,
  startingat,
  cast(_sdc_received_at as date) as update_at,
 from {{ ref('src_mongodb_subscriptions')}}  
), 

max_date as ( 
SELECT
  user_id as last_user_id ,
  max(cast(_sdc_received_at as date)) as max_update_at
 from {{ ref('src_mongodb_subscriptions')}}  
 group by 1
)

select 
   distinct 
   data_info.user_id, 
   data_info.formula, 
   data_info.startingat ,
   case when data_info.startingat is null then 'No' else 'Yes' end as subscription_status,
   max_date.last_user_id,
   max_date.max_update_at
   from data_info 
   inner join max_date 
   on data_info.update_at = max_date.max_update_at and data_info.user_id = max_date.last_user_id
   order by  data_info.user_id asc 

