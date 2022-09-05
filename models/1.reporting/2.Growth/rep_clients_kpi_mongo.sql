

{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

WITH user_data AS (
SELECT _id AS user_id,
(concat(UPPER(lastname),' ',INITCAP(firstname))) as name,
role,
  godfather,
  email,
  phone,
  createdat,
  updatedat,
  comments,
  newsletter,
  last4,
  iat,
  formula
from
    {{ ref('src_mongodb_users') }}
),
subscription AS (
SELECT user_id as user_id_subscription,
allergies_oysters,
subscription_date_mongo AS subscription_date,
subscription_status,
rate,
  allergies_crustaceans  ,
  allergies_shells ,
  allergies_fishes  ,
  allergies_others ,
  allergies_invalid,
ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY subscription_date_mongo DESC
   ) rn
FROM {{ ref('stg_subscription_consolidation') }}
),
current_subscription AS (
SELECT * FROM subscription 
WHERE rn =1
), 

sale_data AS (
     SELECT 
        user_id as user_id_sale_data, 
        place_name, 
        place_id , 
        place_address, 
        place_city , 
        sum(price_ttc) as monetary, 
        date_diff(current_date(), CAST(max(createdat) AS DATE),day) as recence, 
        date_diff(current_date(), CAST(min(createdat) AS DATE),day) as anciennete,         
        case when date_diff( current_date(), CAST(max(createdat) AS DATE), day) > 90 then 'Churn' else 'Retain' end as customer_status,         
        count(distinct subscriptionid) as subscriptions, 
        count(subscriptionid) as subscriptions_occurence, 
        min(createdat) as first_payment , 
        sum(price_ttc) as customer_revenue, 
        max(createdat) as last_payment ,  
        sum(case when type_sale = 'abonnement' then price_ttc end ) as total_subscriptions,
        sum(case when type_sale = 'shop' then price_ttc end ) as total_shop,
        sum(case when type_sale = 'Petit plus' then price_ttc end ) as total_petitplus,
        sum(case when EXTRACT(YEAR FROM CAST(createdat AS DATE))  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE)) then price_ttc end ) as total_year,
        sum(case when EXTRACT(YEAR FROM CAST(createdat AS DATE))  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE))-1 then price_ttc end ) as total_last_year,
        sum(refundedprice) as amount_refunded,
        sum(subscription_total_casiers) as nb_casiers,
        sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count(case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end) as panier_moyen_hors_casier
        FROM {{ ref('stg_mongo_sale_consolidation') }}
        group by 1,2,3,4,5)
SELECT * FROM user_data LEFT JOIN current_subscription ON user_data.user_id = current_subscription.user_id_subscription
LEFT JOIN sale_data ON user_data.user_id = sale_data.user_id_sale_data
WHERE monetary IS NOT NULL
ORDER BY subscription_date