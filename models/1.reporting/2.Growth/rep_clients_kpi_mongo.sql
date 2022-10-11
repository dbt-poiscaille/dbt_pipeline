

{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

-- Récupération et consolidation des données utilisateurs 
WITH user_data AS (
select 
_id as user_id,
concat ( 'https://poiscaille.fr/kraken/client/', _id) as link_kraken,
concat(INITCAP(firstname),' ',INITCAP(lastname)) as name,
INITCAP(firstname) as firstname,
INITCAP(lastname) as lastname,
SUBSTR(INITCAP(lastname),0,1) as firstname_zendesk , 
concat(INITCAP(firstname),' ',SUBSTR(INITCAP(lastname),0,1)) as name_zendesk ,
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
 group by 1,2,3,4,5,6,7,8,9,10,11,12
),

-- Récupération des abonnements 
subscription AS (
SELECT 
  user_id as user_id_subscription,
  allergies_oysters,
  allergies_crustaceans  ,
  allergies_shells ,
  allergies_fishes  ,
  allergies_others ,
  allergies_invalid,
  createdat as subscription_createdat,
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

-- Récupération et consolidation des coupons
users_coupons as ( 
  select 
      customer, 
      nb_coupons,
      coupons_amount, 
      last_coupon
     from  {{ ref('stg_coupons_users_consolidation') }}
),

-- Récupération de tous les clients ayant un discount (50% sur tous les casiers)
users_discount as (
SELECT
  distinct 
  id as discount_user_id,
  email as discount_user_email ,
  discount_coupon_id,
  discount_coupon_name,
  discount_coupon_percentoff,
FROM
  {{ ref('src_stripe_customers') }}
WHERE
  discount_coupon_id = 'uUm1gzIT'
order by id asc 
), 

-- Récupération de la première date de souscription
users_first_subscriptions as (
select
  user_id as users_first_id,
  min(cast(subscribed as date)) as min_subscribed
from 
  {{ ref('stg_subscription_consolidation') }}
  where user_id is not null 
  group by 1 
), 

-- Récupération des données CA et Remboursements Stripe
stripe_ca_refund as (
  SELECT
  stripe_customer_id,
  receipt_email,
  round(sum(charges_amount),2) as ca_global_stripe,
  round(sum(amount_refunded),2) as refund_global_stripe,  
  round(sum(charges_amount),2) - round(sum(amount_refunded),2) as final_ca_stripe
from 
 {{ ref('stg_charges_consolidation') }}
group by 1,2
), 

-- Consolidation des données transactionnelles  
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
        max(place_openings_schedule) as place_openings_schedule,
        case when max(nom_region) = 'Île-de-France' then 'Ile-de-France' else 'Hors IdF' end as localisation, 
        max(zone) as zone_vacances_scolaire,
        round(sum(price_ttc),2) as monetary,
        count(distinct sale_id) as total_transactions, 
        count(distinct subscriptionid) as subscriptions, 
        count(subscriptionid) as subscriptions_occurence, 
        min(sale_date) as first_payment , 
        round(sum(price_ttc),2) as customer_revenue, 
        max(sale_date) as last_payment ,  
        max(case when type_sale = 'Abonnement' then sale_date end) as last_subscription_date,
        max(case when type_sale = 'Boutique' then sale_date end) as last_shop_date,
        round(sum(case when type_sale = 'Abonnement' then price_ttc end ),2) as total_subscriptions,
        round(sum(case when type_sale = 'Boutique' then price_ttc end ),2) as total_shop,
        round(sum(case when type_sale = 'Petit plus' then price_ttc end),2) as total_petitplus,
        round(sum(price_ttc),2) as total_ca_global,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE)) then price_ttc end ),2) as total_year,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE))-1 then price_ttc end ),2) as total_last_year,
        round(sum(amount_refund),2) as amount_refunded,
        round(sum(price_ttc),2) as ca_global,
        max(subscription_total_casiers) as nb_casiers,
        round(sum(price_ttc)/count(distinct sale_id),2) as pan_moy,
        round((sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count( distinct case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end)),2) as panier_moyen_hors_casier_1,
        round(sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count( distinct sale_id),2) as panier_moyen_hors_casier_2
        FROM {{ ref('stg_mongo_sale_consolidation') }}
        group by 1),

user_type as (
  select * from {{ ref('stg_users_subscription_type') }}
),

result as (
  SELECT *,
    'France' as country, 

    /*case 
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
        */

    case 
        when total_transactions = 1 and (nb_casiers = 0 or (nb_casiers = 1 and user_id_subscription is not null)) and recence < 90 then 'Premiere transaction'
        when total_transactions > 1 and total_shop > 0 and  (total_subscriptions = 0 or user_type.user_status_ = 'ancien client') and recence < 90 then 'Client Boutique'
        when total_transactions > 1 and total_subscriptions > 0 and user_type.user_status = 'subscriber' then 'Abonné' 
        when total_transactions > 1 and total_ca_global > 2000 and user_type.user_status = 'subscriber' and nb_casiers > 25 or nb_godsons > 10 or pan_moy > 100 then 'Client Promoteur'
        when total_transactions > 1 and total_ca_global > 4000 and amount_refunded < 200 and user_type.user_status = 'subscriber' then 'Méga-Abonné'
        when (
          user_type.user_status_ = 'ancien client' 
          and (
            last_shop_date < last_subscription_date
            --& add: pas d'achat boutique après résilliation dans les 3 derniers mois
          )
        )
          or (
            total_subscriptions = 0
            and total_shop > 0 
            and recence > 90
          ) 
          then 'Ancien client'
        else 'Autres' end as user_phase_transaction,  
    case 
        when localisation = 'Ile-de-France' and place_openings_day = 'Jeudi' then 'Jeudi'    
        when localisation = 'Ile-de-France' and place_openings_day = 'Vendredi' then 'Vendredi'    
        when localisation = 'Ile-de-France' and place_openings_day = 'Mardi' then 'Mardi'    
        when localisation = 'Ile-de-France' and place_openings_day = 'Mercredi' then 'Mercredi'    
        --when localisation = 'Ile-de-France' and place_openings_day = 'Samedi' then 'Samedi' 
        when localisation = 'Hors IdF' and place_openings_day = 'Jeudi' then 'Mercredi' 
        when localisation = 'Hors IdF' and place_openings_day = 'Vendredi' then 'Jeudi' 
        when localisation = 'Hors IdF' and place_openings_day = 'Mardi' then 'Lundi' 
        when localisation = 'Hors IdF' and place_openings_day = 'Mercredi' then 'Mardi'  
        when localisation = 'Hors IdF' and place_openings_day = 'Samedi' then 'Vendredi'
        end as place_openings_day_preparation, 
        ca_global_stripe - refund_global_stripe as reel_ca_global_stripe,
    case when place_name = 'Livraison à domicile' then 'Domicile' else 'Point Relais' end as  place_type
      FROM user_data LEFT JOIN current_subscription ON user_data.user_id = current_subscription.user_id_subscription
  LEFT JOIN sale_data ON user_data.user_id = sale_data.user_id_sale_data
  left join users_coupons on user_data.customer_id_stripe = users_coupons.customer
  left join user_type on user_data.user_id = user_type.user_type_user_id
  left join stripe_ca_refund on user_data.customer_id_stripe = stripe_ca_refund.stripe_customer_id
  left join users_discount on user_data.customer_id_stripe = users_discount.discount_user_id 
  left join users_first_subscriptions on user_data.user_id = users_first_subscriptions.users_first_id
  ORDER BY user_id asc 
)

select * from result
-- where last_shop_date is not null
-- and user_id = '5ee60116895fb442ebeadccf'



























































