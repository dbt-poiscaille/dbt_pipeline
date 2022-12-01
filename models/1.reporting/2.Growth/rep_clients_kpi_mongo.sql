

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
subscription_raw_data AS (
SELECT
  user_id as user_id_subscription,
  id as subscription_id,
  allergies_oysters,
  allergies_crustaceans  ,
  allergies_shells ,
  allergies_fishes  ,
  allergies_others ,
  allergies_invalid,
  createdat as subscription_createdat,
  subscription_date_mongo AS subscription_date,
  subscription_status,
  formula as subscription_formula,
  rate,
  cast(upcoming_shippingat as date) as next_locker_preparation_date,
  cast(upcoming_deliveryat as date) as next_locker_delivery_date,
  _sdc_sequence,
  cast(startingat as date) as subscription_startingat,
  cast(lastingat as date) as subscription_lastingat,

  case
    when subscription_status = 'Active' then 'Abonne'
    when subscription_status = 'Cancelled' then 'Ancien Abonne'
  end as user_status_,
  
  case
    when subscription_status = 'Active' then 'subscriber'
    when subscription_status = 'Cancelled' then '92366307'
  end as user_status,

  case when rate = 'biweekly' then 'Livraison chaque quinzaine'
       when rate = 'weekly' then 'Livraison chaque semaine'
       when rate = 'monthly' then 'Livraison chaque mois'
  end as subscription_type,
  unsubscribed_reason,

  -- order data by user id and subscription id to select the most recent data of each
  ROW_NUMBER() OVER (
        PARTITION BY user_id,id
        ORDER BY _sdc_sequence DESC
    ) rn
FROM {{ ref('stg_subscription_consolidation') }}
),

-- only get subscription data with rn = 1, i.e the most recent record for each subscription
subscription as (
  select
    *
  from subscription_raw_data
  where rn = 1
),

count_active_subscription as (
  select
    user_id_subscription,
    count(distinct subscription_id) as current_active_subscriptions
  from subscription
  where subscription_status = 'Active'
  and subscription_formula = 'subscription'
  group by 1
),

-- select the nearest upcoming date as next locker date

next_locker_date_data as (
  select distinct
    user_id_subscription,
    min(next_locker_preparation_date) as next_locker_preparation_date,
    min(next_locker_delivery_date) as next_locker_delivery_date
  from subscription
  group by 1
),

-- select the most recent active subscription to be showed
active_subscription_rn as (
  select
    *
    except (subscription_status),
    'Active' as subscription_status,
    row_number() over (
      partition by user_id_subscription
      order by _sdc_sequence desc
    ) as rn_current_subscription
  from subscription
  where 
    subscription_formula = 'subscription'
    -- User can have more than one subscription, it need to filter on active subscription in case the user cancel the most recent subscription (in that case the other subscriptions still active)
    and subscription_status = 'Active'
),

current_active_subscription as (
  select distinct
    *
  from active_subscription_rn
  where 
    rn_current_subscription = 1
),

current_cancelled_subscription_rn as (
  select
    * except (subscription_status),
    case
      when user_id_subscription not in (select distinct user_id_subscription from current_active_subscription) then 'Cancelled'
    end as subscription_status,
    row_number() over (
      partition by user_id_subscription
      order by _sdc_sequence desc
    ) as rn_cancelled_subscription
  from subscription
  where 
    subscription_status = 'Cancelled'
    and subscription_formula = 'subscription'
),

current_cancelled_subscription as (
  select
    *,
  from current_cancelled_subscription_rn
  where rn_cancelled_subscription = 1
  and subscription_status = 'Cancelled'
  -- case subscription_status = null i.e client cancelled one of their active subscriptions
),

current_subscription as (
  select * except (next_locker_preparation_date,next_locker_delivery_date) 
  from current_active_subscription

  union all 
  select * except (next_locker_preparation_date,next_locker_delivery_date) 
  from current_cancelled_subscription
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

-- Final model subscription to join with user data
subscription_final_data as (
  select
    current_subscription.*,
    current_active_subscriptions,
    min_subscribed,
    next_locker_preparation_date,
    next_locker_delivery_date
  from current_subscription
  left join count_active_subscription on current_subscription.user_id_subscription = count_active_subscription.user_id_subscription
  left join users_first_subscriptions on current_subscription.user_id_subscription = users_first_subscriptions.users_first_id
  left join next_locker_date_data on current_subscription.user_id_subscription = next_locker_date_data.user_id_subscription
),

-- Récupération et consolidation des coupons
users_coupons as ( 
  select 
      customer, 
      nb_coupons,
      coupons_amount, 
      last_coupon,
      coupon_source
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


-- Récupération des données CA et Remboursements Stripe
stripe_ca_refund as (
  SELECT distinct
  stripe_customer_id,
  -- receipt_email,
  round(sum(charges_amount),2) as ca_global_stripe,
  round(sum(amount_refunded),2) as refund_global_stripe,  
  round(sum(charges_amount),2) - round(sum(amount_refunded),2) as final_ca_stripe
from 
 {{ ref('stg_charges_consolidation') }}
group by 1
), 

-- Consolidation des données transactionnelles  
sale_data AS (
     SELECT distinct
        user_id as user_id_sale_data, 

        case when date_diff( current_date(), max(sale_date), day) > 90 then 'Churn' else 'Retain' end as customer_status,         
        date_diff(current_date(), max(sale_date) ,day) as recence, 
        date_diff(current_date(), min(sale_date) ,day) as anciennete,    
        max(place_name) as place_name, 
        max(place_id) as place_id, 
        ifnull(max(place_address),max(shipping_addresse)) as place_address, 
        max(nom_departement) as place_departement, 
        max(nom_region) as place_region, 
        ifnull(max(place_city),max(shipping_city))  as place_city, 
        ifnull(max(place_codepostal),max(shipping_codepostal)) as place_codepostal, 
        max(place_openings_day) as place_openings_day,
        max(place_openings_schedule) as place_openings_schedule,
        case when max(nom_region) = 'Île-de-France' then 'Ile-de-France' else 'Hors IdF' end as localisation, 
        max(zone) as zone_vacances_scolaire,
        -- round(sum(sale_total_ttc),2) as monetary,
        count(distinct sale_id) as total_transactions, 
        count(distinct subscriptionid) as subscriptions, 
        count(subscriptionid) as subscriptions_occurence, 
        min(sale_date) as first_payment , 
        round(sum(sale_total_ttc),2) as customer_revenue, 
        max(sale_date) as last_payment ,  
        max(case when type_sale = 'Abonnement' then sale_date end) as last_subscription_date,
        max(case when type_sale = 'Boutique' then sale_date end) as last_shop_date,
        round(sum(case when type_sale = 'Abonnement' then sale_locker_ttc end ),2) as total_subscriptions,
        round(sum(case when type_sale = 'Boutique' then sale_boutique_ttc end ),2) as total_shop,
        round(sum(case when type_sale = 'Petit plus' then sale_bonus_ttc end),2) as total_petitplus,
        round(sum(sale_total_ttc),2) as total_ca_global,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE)) then sale_total_ttc end ),2) as total_year,
        round(sum(case when EXTRACT(YEAR FROM sale_date)  = EXTRACT(YEAR FROM CAST(CURRENT_DATE() AS DATE))-1 then sale_total_ttc end ),2) as total_last_year,
        round(sum(amount_refund),2) as amount_refunded,
        round(sum(sale_total_ttc),2) as ca_global,
        max(subscription_total_casiers) as nb_casiers,
        round(sum(sale_total_ttc)/count(distinct sale_id),2) as pan_moy,
        round((sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count( distinct case when type_sale = 'shop' or type_sale = 'Petit plus' then sale_id end)),2) as panier_moyen_hors_casier_1,
        round(sum(case when type_sale = 'shop' or type_sale = 'Petit plus' then price_ttc end )/count( distinct sale_id),2) as panier_moyen_hors_casier_2,
        round(avg(avg_command_score),1) as avg_score_command_client,
        
        FROM {{ ref('stg_mongo_sale_consolidation') }}
        group by 1),

-- cp_source as (
--   select distinct
--     user_id as user_id_cp,
--     first_value(coupon_source) over (partition by user_id order by count_cp_src desc) as coupon_source,
--   from (
--     select
--       user_id,
--       coupon_source,
--       count(coupon_source) as count_cp_src
--     from {{ ref('stg_mongo_sale_consolidation') }}
--     group by 1,2
--   )
-- ),

result as (
  SELECT * except (user_status,user_status_,rn,rn_current_subscription, subscription_type),
    'France' as country, 

    case 
        when total_transactions = 1 and (nb_casiers = 0 or (nb_casiers = 1 and user_id_subscription is not null)) and recence < 90 then 'Premiere transaction'
        when total_transactions > 1 and total_shop > 0 and  (total_subscriptions = 0 or subscription_final_data.user_status_ = 'Ancien Abonne') and recence < 90 then 'Client Boutique'
        when total_transactions > 1 and total_subscriptions > 0 and subscription_final_data.user_status = 'subscriber' then 'Abonné' 
        when total_transactions > 1 and total_ca_global > 2000 and subscription_final_data.user_status = 'subscriber' and nb_casiers > 25 or nb_godsons > 10 or pan_moy > 100 then 'Client Promoteur'
        when total_transactions > 1 and total_ca_global > 4000 and amount_refunded < 200 and subscription_final_data.user_status = 'subscriber' then 'Méga-Abonné'
        when (
          subscription_final_data.user_status_ = 'Ancien Abonne' 
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
      when subscription_final_data.user_status_ is null then 'Sans Abonnement'
      else subscription_final_data.user_status_
    end as user_status_,

    case
      when subscription_final_data.user_status_ = 'Abonne' then 'subscriber' -- subscriber = Abonne
      when subscription_final_data.user_status_ = 'Ancien Abonne' and recence <= 90 then 'customer' -- customer = Client
      when subscription_final_data.user_status_ = 'Ancien Abonne' and (recence > 90 or recence is null) then '92366307' -- 92366307 = Ancien client
      -- when total_transactions > 1 and total_ca_global > 4000 and amount_refunded < 200 and subscription_final_data.user_status_ = 'Abonne' then 'other'  -- other = Mega-Abonne
      else 'lead' -- lead = Lead
    end as user_status,
    
    case
      when subscription_status = 'Active' then subscription_type
      else null
    end as subscription_type,

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
    
    case when place_name = 'Livraison à domicile' then 'Domicile' else 'Point Relais' end as  place_type,

    -- key dates

    date_sub(next_locker_preparation_date, interval 1 day) as next_locker_choice_date,

    -- date_add(next_locker_date, interval 1 day) as next_locker_preparation_date,
    -- case
    --   when place_name <> 'Livraison à domicile' and localisation = 'Ile-de-France' then date_add(next_locker_date, interval 1 day) -- Point relais
    --   when place_name = 'Livraison à domicile' and localisation = 'Ile-de-France' then date_add(next_locker_date, interval 2 day) -- Domicile
    --   when localisation <> 'Ile-de-France' then date_add(next_locker_date, interval 2 day)
    --   else date_add(next_locker_date, interval 2 day)
    -- end as next_locker_delivery_date,
  
  FROM user_data 
  LEFT JOIN subscription_final_data ON user_data.user_id = subscription_final_data.user_id_subscription
  LEFT JOIN sale_data ON user_data.user_id = sale_data.user_id_sale_data
  left join users_coupons on user_data.customer_id_stripe = users_coupons.customer
  left join stripe_ca_refund on user_data.customer_id_stripe = stripe_ca_refund.stripe_customer_id
  left join users_discount on user_data.customer_id_stripe = users_discount.discount_user_id 
  ORDER BY user_id asc 
)

select * from result
-- where last_shop_date is not null
-- where user_id = '5ee60116895fb442ebeb201c'



























































