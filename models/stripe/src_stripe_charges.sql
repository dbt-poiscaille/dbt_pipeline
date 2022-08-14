{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

SELECT
  id,
  receipt_email,
  balance_transaction,
  created,
  payment_intent,
  receipt_number,
  paid,
  invoice,
  currency,
  payment_method,
  receipt_url,
  failure_code,
  failure_message,
  status,
  refunds,
  refunded,
  captured,
  source.exp_year,
  source.object,
  source.country,
  source.id,
  source.name,
  source.customer as source_customer,
  source.address_country as source_country,
  amount,
  amount_refunded,
  disputed,
  updated,
  customer,
  dispute,
  shipping.address.country,
  shipping.address.city,
  shipping.address.state,
  shipping.address.postal_code
FROM
{{ source('stripe', 'charge') }}