{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

SELECT
  distinct 
  id,
  email,
  livemode,
  name,
  description,
  currency,
  cast(discount_coupon_created as date) as discount_coupon_created,
  discount_coupon_id,
  discount_coupon_name,
  discount_coupon_percentoff,
  discount_coupon_valid,
  discount_coupon_livemode,
  discount_coupon_currency,
  discount_coupon_amountoff,
  discount_customer, 
  FROM
  {{ ref('src_stripe_customers') }}
WHERE
    discount_coupon_valid is true
order by id asc