{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
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
),

sale_detail as (
SELECT
  sale_date,
  sale_id,
  place_id,
  customerid,
  email,
  type_sale,
  sale_boutique_ttc,
  sale_locker_ttc,
  sale_bonus_ttc
  -- price_details_ttc,
  --offerings_value_items_value_product_type,
  --offerings_value_name
FROM {{ ref('stg_mongo_sale_consolidation')}} 
)

SELECT
distinct 
  sale_detail.*,
  data_cp.customer,
  data_cp.description,
  ROUND(data_cp.in_amount/100,2) as coupon_value,
  CASE WHEN data_cp.customer IS NOT NULL THEN 'True' ELSE 'False' END as coupon_used
FROM 
  sale_detail
LEFT JOIN data_cp
ON 
  sale_detail.sale_date = data_cp.in_date
  AND sale_detail.customerid = data_cp.customer

WHERE data_cp.customer IS NOT NULL
ORDER BY sale_detail.sale_date, sale_detail.sale_id