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
) 

select   
    in_date,
    description as cp_name, 
    count(distinct customer) as nb_customer,
    round(sum(in_amount)/100,2) as total_amount
    from data_cp
    group by 1,2
    order by total_amount desc 