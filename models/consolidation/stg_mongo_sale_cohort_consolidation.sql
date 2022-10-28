{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with sale_consolidation as (
    select distinct
        sale_id,
        user_id,
        firstname,
        lastname,
        email,
        subscription_id,
        customerid,
        subscription_rate,
        subscription_status,
        subscription_type,
        type_sale,
        sale_boutique_ttc,
        sale_locker_ttc,
        sale_bonus_ttc,
        sale_total_ttc,

        extract(year from sale_date) as sale_year,
        extract(month from sale_date) as sale_month,
        sale_date,
        prev_sale_date,
        date_diff(sale_date,prev_sale_date,month) as months_from_prev_charge,
        date_diff(sale_date,prev_sale_date,day) as days_from_prev_charge,
        prev_subscription_sale_date,
        prev_shop_sale_date,
        user_transaction_phase,

        place_city,
        nom_departement,
        nom_region,
        type_livraison
    from {{ ref('stg_mongo_sale_consolidation') }}
),

t_first_purchase AS (
  SELECT distinct
  sale_date,
  DATE_DIFF(sale_date, first_purchase_date, MONTH) AS month_order,
  first_purchase_date AS first_purchase_date,
  user_id
  FROM (
    SELECT 
     sale_date,
     user_id,
    FIRST_VALUE(DATE(TIMESTAMP(sale_date))) OVER (PARTITION BY user_id ORDER BY DATE(TIMESTAMP(sale_date))) AS first_purchase_date
     from sale_consolidation
      where type_sale != 'Boutique'  
    )
  ),

contact_cohort as (
    select distinct
        _id as user_id,
        cast(createdat as date) as contact_createdat
    from {{ ref('src_mongodb_users') }}
)

select distinct
    sale_consolidation.*,
    contact_createdat,
    first_purchase_date,
    extract(month from first_purchase_date) as first_purchase_month,
    extract(year from first_purchase_date) as first_purchase_year,
    date_diff(sale_consolidation.sale_date,first_purchase_date,month) as months_from_first_purchase,


from sale_consolidation
left join contact_cohort
on sale_consolidation.user_id = contact_cohort.user_id
left join t_first_purchase
on sale_consolidation.user_id = t_first_purchase.user_id