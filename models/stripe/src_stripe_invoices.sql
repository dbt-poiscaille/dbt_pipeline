{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


select
  id,
  customer,
  invoice,
  currency,
  quantity,
  subscription,
  unit_amount_decimal,
  proration,
  livemode,
  amount,
  date,
  updated,
  plan.currency,
  plan.id,
  plan.usage_type,
  plan.product,
  plan.INTERVAL,
  plan.interval_count,
  plan.amount_decimal,
  plan.amount,
  plan.billing_scheme,
  plan.active,
  unit_amount,
  period,
  period.start,
  period.END,
  _sdc_batched_at,
  _sdc_extracted_at,
  description,
  updated_by_event_type,
  amount
FROM
  {{ source('stripe', 'invoice_items') }}

  where subscription = 'sub_JwWKkMm5hmlSL1'



 