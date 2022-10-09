{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


with data_global as (
select 
 *
  from {{ ref('stg_mongo_sale_consolidation') }}
) 

select 
    user_id,
    count(distinct case when type_sale = 'Abonnement' then sale_id end ) as nb_casiers,
    count(distinct case when type_sale = 'Boutique' then sale_id end ) as nb_shop, 
    max(subscription_total_casiers) as total_casiers ,
    case 
      when  max(subscription_total_casiers) = 1 then 'Commande Unique' 
      when  max(subscription_total_casiers) is null then 'Client Boutique'
      else 'Mutli Commande' end as customer_type
   from data_global
   group by 1 
