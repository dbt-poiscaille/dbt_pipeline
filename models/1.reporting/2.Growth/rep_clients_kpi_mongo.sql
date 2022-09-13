

{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

WITH user_data AS (
select 
_id as user_id,
(concat(UPPER(lastname),' ',INITCAP(firstname))) as name,
INITCAP(firstname) as firstname,
INITCAP(lastname) as lastname,
role,
  'Utilisateur B2C' as contact_type,
  phone_fixe_f as phone_fixe, 
  phone_mobile_f as phone_mobil,
  cast(createdat as date) as createdat,
  max(godfather) as godfather,
  count(distinct godson) as nb_godsons,
  max(godson) as godson,
  max(email) as email,
  max(comments) as comments,
  max(newsletter) as newsletter,
  max(last4) as last4,
  max(customer) as customer_id_stripe, 
  max(formula) as formula
from
    {{ ref('src_mongodb_users') }}
 group by 1,2,3,4,5,6,7,8,9
),
subscription AS (
SELECT user_id as user_id_subscription,
allergies_oysters,
subscription_date_mongo AS subscription_date,
subscription_status,
rate,
  case when rate = 'biweekly' then 'Livraison chaque quinzaine'
       when rate = 'weekly' then 'Livraison chaque semaine'
       when rate = 'monthly' then 'Livraison chaque mois'
       end as subscription_type,
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
        case when date_diff( current_date(), CAST(max(createdat) AS DATE), day) > 90 then 'Churn' else 'Retain' end as customer_status,         
        date_diff(current_date(), CAST(max(createdat) AS DATE),day) as recence, 
        date_diff(current_date(), CAST(min(createdat) AS DATE),day) as anciennete,    
        max(place_name) as place_name, 
        max(place_id) as place_id, 
        max(place_address) as place_address, 
        max(place_city)  as place_city, 
        max(place_codepostal) as place_codepostal, 
        case 
          when max(place_openings_day) = 'Thursday' then 'Jeudi' 
          when max(place_openings_day) = 'Friday' then 'Vendredi' 
          when max(place_openings_day) = 'Tuesday' then 'Mardi' 
          when max(place_openings_day) = 'Wednesday' then 'Mercredi' 
          when max(place_openings_day) = 'Saturday' then 'Samedi' 
          end as place_openings_day,
        case when max(nom_region) = 'Île-de-France' then 'IDF' else 'Région' end as localisation, 
        max(zone) as zone_vacances_scolaire,
        round(sum(price_ttc),2) as monetary,
        count(distinct subscriptionid) as subscriptions, 
        count(subscriptionid) as subscriptions_occurence, 
        min(createdat) as first_payment , 
        round(sum(price_ttc),2) as customer_revenue, 
        max(createdat) as last_payment ,  
        round(sum(case when type_sale = 'abonnement' then price_ttc end ),2) as total_subscriptions,
        round(sum(case when type_sale = 'shop' then price_ttc end ),2) as total_shop,
        round(sum(case when type_sale = 'Petit plus' then price_ttc end ),2) as total_petitplus,
        round(sum(case when EXTRACT(YEAR FROM CAST(createdat AS DATE))  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE)) then price_ttc end ),2) as total_year,
        round(sum(case when EXTRACT(YEAR FROM CAST(createdat AS DATE))  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE))-1 then price_ttc end ),2) as total_last_year,
        round(sum(refundedprice),2) as amount_refunded,
        sum(subscription_total_casiers) as nb_casiers,
        round(sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count(case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end),2) as panier_moyen_hors_casier
        FROM {{ ref('stg_mongo_sale_consolidation') }}
        group by 1)

SELECT *,
  case 
   when total_subscriptions is null and total_shop is null then 'lead'
   when total_subscriptions is not null then 'subscriber'
   when total_subscriptions is null and total_shop >0 then 'customer'
   end as client_lifecycle, 
   -- Demander à Yves comment récupérer les données de jour de préparation
  case when place_name = 'Livraison à domicile' then 'Domicile' else 'PR' end as  place_type
    FROM user_data LEFT JOIN current_subscription ON user_data.user_id = current_subscription.user_id_subscription
LEFT JOIN sale_data ON user_data.user_id = sale_data.user_id_sale_data
--WHERE monetary IS NOT NULL
ORDER BY subscription_date



































































