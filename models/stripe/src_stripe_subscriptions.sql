{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

SELECT
  id,
  customer,
  created,
  trial_end,
  ended_at,
  canceled_at,
  start,
  status,
  current_period_start,
  current_period_end,
  quantity,
  latest_invoice,
  billing_cycle_anchor,
  livemode,
  start_date,
  updated,
  billing,
  items,
  collection_method,
  plan
from {{ source('stripe', 'subscriptions') }}