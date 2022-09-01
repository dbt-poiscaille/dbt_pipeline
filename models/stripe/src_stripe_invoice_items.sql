{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}
select
  distinct 
  cast(date as date) in_date, 
  customer,
  description,
  object as in_object,
  invoice as in_invoice,
  discountable as in_discountable,
  currency in_currency,
  id as in_id,
  quantity as in_quantity,
  subscription as in_subscription_id,
  unit_amount_decimal in_unit_amount,
  subscription_item as in_subcsription_item,
  livemode as in_livemode,
  amount as in_amount,
  --updated as in_updated,
  cast(plan.created as date) as in_plan_created,
  plan.currency as in_plan_currency,
  plan.id as in_plan_id,
  plan.usage_type as in_plan_usage_type,
  plan.product as in_plan_product,
  plan.interval as in_plan_interval,
  plan.interval_count as in_plan_interval_count,
  plan.amount_decimal as in_amount_decima,
  plan.livemode as in_plan_livemode,
  plan.amount as in_plan_amount,
  plan.active as in_plan_active,
  plan.name as in_plan_name,
  cast(period.start as date) as in_period_start,
  cast(period.END as date) as in_period_end

from 
  {{ source('stripe', 'invoice_items') }}

where livemode is true   



 


























