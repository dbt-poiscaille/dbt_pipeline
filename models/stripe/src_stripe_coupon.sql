
{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

SELECT * FROM {{ source('stripe', 'coupons') }}
