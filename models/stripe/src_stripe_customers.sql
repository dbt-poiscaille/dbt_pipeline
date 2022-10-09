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
  discount.coupon.created as discount_coupon_created,
  discount.coupon.id as discount_coupon_id,
  discount.coupon.name as discount_coupon_name,
  discount.coupon.percent_off as discount_coupon_percentoff,
  discount.coupon.valid as discount_coupon_valid,
  discount.coupon.livemode as discount_coupon_livemode,
  discount.coupon.currency as discount_coupon_currency,
  discount.coupon.amount_off as discount_coupon_amountoff,
  discount.customer  as discount_customer
  from {{ source('stripe', 'customers') }}
  