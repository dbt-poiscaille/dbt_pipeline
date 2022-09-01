{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select
  cast(created as date) as day,
  id,
  amount,
  currency,
  source,
  type
   from  {{ source('stripe', 'balance_transactions') }}
order by day desc
