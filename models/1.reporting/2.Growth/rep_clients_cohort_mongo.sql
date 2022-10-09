{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}
-- Analyse de cohort que sur les abonnements , début de données 072022

WITH t_first_purchase AS (
  SELECT 
  sale_date,
  DATE_DIFF(sale_date, first_purchase_date, MONTH) AS month_order,
  FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
  user_id
  FROM (
    SELECT 
     sale_date,
     user_id,
    FIRST_VALUE(DATE(TIMESTAMP(sale_date))) OVER (PARTITION BY user_id ORDER BY DATE(TIMESTAMP(sale_date))) AS first_purchase_date
     from {{ ref('stg_mongo_sale_consolidation') }}
      where type_sale != 'Boutique'  
    )
  ),

/* This table computes the aggregate customer count per first purchase cohort and month order */
t_agg AS (
  SELECT 
  first_purchase,
  month_order,
  COUNT(DISTINCT user_id) AS Customers
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
