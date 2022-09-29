
{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}


select
* from {{ ref('stg_ga_boutique_tunnel') }}
union all 
select 
* from {{ ref('stg_ga_subscriptions_tunnel') }}