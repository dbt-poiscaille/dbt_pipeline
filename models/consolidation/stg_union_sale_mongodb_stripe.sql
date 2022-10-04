{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

WITH stripe_sale AS (
  SELECT 
    distinct id as sale_id,
    receipt_email as customer_email, 
    amount as amount,
    created as created_at,
    'Stripe' as data_from,
  FROM {{ ref('src_stripe_charges')}}
  --, UNNEST(refunds) as refunds
  WHERE
    created <= '2022-06-30'
    --AND refunds.value.object != 'refund'
),

mongodb_sale AS (
  SELECT  
    sale_id as sale_id,
    email as customer_email,
    offerings_value_items_value_cost_ttc as amount,
    createdat as created_at,
    'MongoDB' as data_from,
  FROM {{ ref('src_mongodb_sale')}}
  WHERE createdat > '2022-06-30'
),

final as (
    SELECT * FROM stripe_sale 
    UNION ALL
    SELECT * FROM mongodb_sale
    ORDER BY created_at desc
)

SELECT
    --sale_id,
    customer_email,
    SUM(amount) as total_amount
FROM final
GROUP BY customer_email