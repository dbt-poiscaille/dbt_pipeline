

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
SELECT 
  user_id as user_id_subscription,
  allergies_oysters,
  allergies_crustaceans  ,
  allergies_shells ,
  allergies_fishes  ,
  allergies_others ,
  allergies_invalid,
  subscription_date_mongo AS subscription_date,
  subscription_status,
  rate,
  case when rate = 'biweekly' then 'Livraison chaque quinzaine'
       when rate = 'weekly' then 'Livraison chaque semaine'
       when rate = 'monthly' then 'Livraison chaque mois'
       end as subscription_type,
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

users_coupons as ( 
  select 
      customer, 
      nb_coupons,
      coupons_amount, 
      last_coupon
     from  {{ ref('stg_coupons_users_consolidation') }}
),

sale_data AS (
     SELECT 
        user_id as user_id_sale_data, 
        case when date_diff( current_date(), max(sale_date), day) > 90 then 'Churn' else 'Retain' end as customer_status,         
        date_diff(current_date(), max(sale_date) ,day) as recence, 
        date_diff(current_date(), min(sale_date) ,day) as anciennete,    
        max(place_name) as place_name, 
        max(place_id) as place_id, 
        max(place_address) as place_address, 
        max(place_city)  as place_city, 
        max(place_codepostal) as place_codepostal, 
        max(place_openings_day) as place_openings_day,
        case when max(nom_region) = 'Île-de-France' then 'Ile-de-France' else 'Hors IdF' end as localisation, 
        max(zone) as zone_vacances_scolaire,
        round(sum(price_details_ttc)/100,2) as monetary,
        count(distinct subscriptionid) as subscriptions, 
        count(subscriptionid) as subscriptions_occurence, 
        min(sale_date) as first_payment , 
        round(sum(price_details_ttc)/100,2) as customer_revenue, 
        max(sale_date) as last_payment ,  
        round(sum(case when type_sale = 'abonnement' then price_details_ttc end )/100,2) as total_subscriptions,
        round(sum(case when type_sale = 'shop' then price_details_ttc end )/100,2) as total_shop,
        round(sum(case when type_sale = 'Petit plus' then price_details_ttc end)/100,2) as total_petitplus,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE)) then price_details_ttc end )/100,2) as total_year,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE))-1 then price_details_ttc end )/100,2) as total_last_year,
        round(sum(refundedprice),2) as amount_refunded,
        round(sum(price_details_ttc)/100,2) as ca_global,
        max(subscription_total_casiers) as nb_casiers,
        round(sum(price_details_ttc)/count(distinct sale_id)/100,2) as pan_moy,
        round((sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_details_ttc end )/count( distinct case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end))/100,2) as panier_moyen_hors_casier_1,
        round(sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_details_ttc end )/count( distinct sale_id)/100,2) as panier_moyen_hors_casier_2
        FROM {{ ref('stg_mongo_sale_consolidation') }}
        group by 1),

        user_type as (
          select * from {{ ref('stg_users_subscription_type') }}
        )

SELECT *,
  'France' as country, 
  case 
   when total_subscriptions is null and total_shop is null then 'lead'
   when total_subscriptions is not null then 'subscriber'
   when total_subscriptions is null and total_shop >0 then 'customer'
   end as client_lifecycle, 
   case 
      when place_openings_day ='Jeudi' then 'Mercredi'
      when place_openings_day ='Vendredi' then 'Jeudi'
      when place_openings_day ='Mardi' then 'Lundi'
      when place_openings_day ='Mercredi' then 'Mardi'
      when place_openings_day ='Samedi' then 'Vendredi'
      end as place_openings_day_preparation, 

   case 
      when localisation = 'Ile-de-France' and place_openings_day = 'Jeudi' then 'Jeudi'    
      when localisation = 'Ile-de-France' and place_openings_day = 'Vendredi' then 'Vendredi'    
      when localisation = 'Ile-de-France' and place_openings_day = 'Mardi' then 'Mardi'    
      when localisation = 'Ile-de-France' and place_openings_day = 'Mercredi' then 'Mercredi'    
      when localisation = 'Ile-de-France' and place_openings_day = 'Samedi' then 'Samedi' 
      when localisation = 'Hors IdF' and place_openings_day = 'Jeudi' then 'Vendredi' 
      when localisation = 'Hors IdF' and place_openings_day = 'Vendredi' then 'Samedi' 
      when localisation = 'Hors IdF' and place_openings_day = 'Mardi' then 'Mercredi' 
      when localisation = 'Hors IdF' and place_openings_day = 'Mercredi' then 'Jeudi'  
      when localisation = 'Hors IdF' and place_openings_day = 'Samedi' then 'Dimanche'
      end as place_openings_day_livraison,  
         
   -- Demander à Yves comment récupérer les données de jour de préparation
  case when place_name = 'Livraison à domicile' then 'Domicile' else 'Point Relais' end as  place_type
    FROM user_data LEFT JOIN current_subscription ON user_data.user_id = current_subscription.user_id_subscription
LEFT JOIN sale_data ON user_data.user_id = sale_data.user_id_sale_data
left join users_coupons on user_data.customer_id_stripe = users_coupons.customer
left join user_type on user_data.user_id = user_type.user_type_user_id
--where customer_id_stripe = 'cus_Gzy1NAB2RRae79'
ORDER BY user_id asc 




























































