{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

WITH stripe_charge AS (
    SELECT
        customer AS stripe_customer_id,
        receipt_email AS email, 
        SUM(CASE WHEN created <= '2022-06-30' THEN amount ELSE 0 END ) as total_charge_amount_stripe_before_300622,
        SUM(CASE WHEN created > '2022-06-30' THEN amount ELSE 0 END ) as total_charge_amount_stripe_after_300622, 
        SUM(CASE WHEN created <= '2022-06-30' THEN refunds.value.amount ELSE 0 END) as total_refund_amount_stripe_before_300622,
        SUM(CASE WHEN created > '2022-06-30' THEN refunds.value.amount ELSE 0 END) as total_refund_amount_stripe_after_300622,
    FROM 
        {{ ref('src_stripe_charges')}}, 
        UNNEST(refunds) as refunds
    GROUP BY customer, receipt_email
),

mongodb_sale AS (
    SELECT
        customerid as mongodb_customer_id,
        email as email,
        SUM(offerings_value_items_value_cost_ttc) AS total_mongo_sale
    FROM
        {{ ref('src_mongodb_sale')}}
    GROUP BY customerid, email
)

SELECT
  stripe_charge.stripe_customer_id,
  mongodb_sale.mongodb_customer_id,
  stripe_charge.email,
  mongodb_sale.email,
  stripe_charge.total_charge_amount_stripe_before_300622,
  stripe_charge.total_charge_amount_stripe_after_300622,
  stripe_charge.total_refund_amount_stripe_before_300622,
  stripe_charge.total_refund_amount_stripe_after_300622,
  mongodb_sale.total_mongo_sale
FROM stripe_charge FULL OUTER JOIN mongodb_sale
ON 
  stripe_charge.stripe_customer_id = mongodb_sale.mongodb_customer_id
  AND stripe_charge.email = mongodb_sale.email

