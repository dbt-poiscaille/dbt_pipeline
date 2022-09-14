
{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


with data_cp as (
SELECT
  in_date,
  in_id,
  in_invoice,
  in_subscription_id,
  in_amount,
  customer,
  description,
FROM
   {{ ref('src_stripe_invoice_items') }}
WHERE
  LOWER(description) LIKE '%coupon%'
  order by in_date asc 
) 

select 
   customer, 
   count(distinct description) as nb_coupons,
   round(sum(in_amount)/100,2) as coupons_amount, 
   max(description) as last_coupon
   from data_cp
   group by 1
   order by customer asc 

