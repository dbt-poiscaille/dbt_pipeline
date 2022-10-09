{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}


WITH t_first_purchase AS (
  SELECT 
  charge_date,
  DATE_DIFF(charge_date, first_purchase_date, MONTH) AS month_order,
  FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
  stripe_customer_id
  FROM (
    SELECT 
     charge_date,
     stripe_customer_id,
    FIRST_VALUE(DATE(TIMESTAMP(charge_date))) OVER (PARTITION BY stripe_customer_id ORDER BY DATE(TIMESTAMP(charge_date))) AS first_purchase_date
     from {{ ref('stg_charges_consolidation') }}

    )
  ),

/* This table computes the aggregate customer count per first purchase cohort and month order */
t_agg AS (
  SELECT 
  first_purchase,
  month_order,
  COUNT(DISTINCT stripe_customer_id) AS Customers
  FROM 
  t_first_purchase
  GROUP BY first_purchase, month_order
),

/* This table computes the retention rate */
 t_cohort AS (
  SELECT *,
  SAFE_DIVIDE(Customers, CohortCustomers) AS CohortCustomersPerc
  FROM (
      SELECT *,
      FIRST_VALUE(Customers) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortCustomers
      FROM t_agg
  )
 )

SELECT * FROM t_cohort 
ORDER BY first_purchase, month_order
