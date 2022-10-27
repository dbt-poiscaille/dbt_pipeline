
{{
  config(
    materialized = 'table',
    labels = {'type': 'test_sanofie', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select 
    * 
  from 
    {{ ref('stg_funnel_consolidation') }}