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
  source.exp_year AS source_exp_year,
  source.object AS source_object,
  source.country AS source_country,
  source.id AS source_id,
  source.name AS source_name,
  source.customer as source_customer,
  source.address_country as source_address_country,
  amount,
  amount_captured,
  amount_refunded,
  disputed,
  updated,
  customer,
  dispute,
  shipping.address.country AS shipping_address_country,
  shipping.address.city AS shipping_address_city,
  shipping.address.state AS shipping_address_state,
  shipping.address.postal_code AS  shipping_address_postal_code,
  description,
  _sdc_extracted_at
FROM
{{ source('stripe', 'charges') }}