{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

SELECT
  id,
  email,
  currency,
  livemode,
  created,  
  updated,
  tax_exempt,
  subscriptions,
  name,
  description,
  account_balance,
  shipping,
  shipping.phone,
  shipping.name as shipping_name,
  balance,
  next_invoice_sequence,
  default_source,
  ---sources
  from {{ source('stripe', 'customers') }}