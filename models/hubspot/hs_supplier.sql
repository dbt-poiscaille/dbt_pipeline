{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select * from {{ ref('rep_supplier') }}
order by supplier_id asc 