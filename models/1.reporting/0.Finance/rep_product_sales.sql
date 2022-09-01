{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select * from {{ ref('stg_detail_products_sales') }}