{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select 
      * 
     from {{ ref('stg_mongo_sale_consolidation') }}