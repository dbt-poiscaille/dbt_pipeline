{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

SELECT
  stripe_customer_id, 
  stripe_id, 
  receipt_email,
  SUM(charges_amount) AS total_payment,
  SUM(CASE WHEN charge_type = 'Shop' THEN charges_amount ELSE 0 END) shop_payment,
  SUM(CASE WHEN charge_type LIKE '%Abonnement%' THEN charges_amount ELSE 0 END) abonnement_payment,
  SUM(CASE WHEN charge_type = 'PetitPlus' THEN charges_amount ELSE 0 END) PetitPlus_payment,
  SUM(amount_refunded) as sum_refund
  
FROM {{ ref('stg_charges_consolidation') }}
WHERE 
  charge_date BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY stripe_customer_id, stripe_id, receipt_email
ORDER BY stripe_customer_id, stripe_id, receipt_email