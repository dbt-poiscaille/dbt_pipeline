
{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select 
  * from {{ ref('stg_coupons_mongo_data') }}