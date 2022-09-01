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
  -- items,
  collection_method,
  plan.object				AS plan_object,
  plan.created			AS plan_created,
  plan.currency			AS plan_currency	,		
  plan.id	AS plan_id,
  plan.usage_type		AS plan_usage_type,
  plan.product			AS plan_product,
  plan.interval			AS plan_interval,
  plan.interval_count AS plan_interval_count,	
  plan.livemode			AS plan_livemode,
  plan.amount			AS plan_amount,
  plan.billing_scheme AS plan_billing_scheme,
  plan.active		AS plan_active,
  plan.name AS plan_name
from {{ source('stripe', 'subscriptions') }}