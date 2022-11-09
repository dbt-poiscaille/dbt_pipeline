{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


with data_info as (
SELECT
  distinct 
   user_id,
  formula,
  startingat,
  _sdc_sequence,
  _sdc_received_at as update_at,
  --RANK() OVER ( PARTITION BY department ORDER BY startdate ) AS rank

FROM  {{ ref('src_mongodb_subscriptions')}}  
  where formula = 'subscription'
 --WHERE user._id = '62b54775493ad41a010f3a29'
) , 

max_data as (
  select 
        user_id ,
        max ( _sdc_sequence ) as _sdc_sequence,
        from {{ ref('src_mongodb_subscriptions')}}  
        group by 1
  ), 

  user_data as (
      SELECT
         _id as user_id 
        from {{ ref('src_mongodb_users')}}  
  ) ,
  user_type as (
  select 
         data_info.user_id, 
         --data_info.formula,
         data_info.startingat , 
         case when data_info.startingat is null then '92366307' else 'subscriber' end as user_status,
         case when data_info.startingat is null then 'Ancien Abonne' else 'Abonne' end as user_status_,
         -- check client ( ancien client boutique, personne n'ayant rien acheté depuis 3 mois )
         data_info.update_at ,
         max_data._sdc_sequence as _sdc_sequence,
         RANK() OVER ( PARTITION BY data_info.user_id ORDER BY data_info.update_at ) AS rank
         from data_info 
         inner join max_data 
         on data_info.user_id = max_data.user_id and data_info._sdc_sequence = max_data._sdc_sequence
         --where data_info.user_id = '62b54775493ad41a010f3a29'
         order by rank desc , user_id asc 
  )

  select 
       user_data.user_id as user_type_user_id, 
       startingat, 
       case when user_status is null then 'lead' else user_status end as user_status,
       case when user_status_ is null then 'Sans Abonnement' else user_status_ end as user_status_
       
       from user_data
       left join user_type
       on user_data.user_id = user_type.user_id






