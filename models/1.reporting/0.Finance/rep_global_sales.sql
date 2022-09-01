{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}



select * from {{ ref('stg_charges_consolidation') }}
where charges_amount > 0 