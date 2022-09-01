{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'production'}  
   )
}}


with data_consolidation_users as (

select
  _id as user_id_mongodb,
  customer as user_id_stripe,
  case when customer is null then 'No StripeId' else 'StripeId'end id_stripe_status,
  --case when customer is null then 'Prospect' else 'Customers'end users_type,
  formula as type_abo,
  case when formula is null then 'Non Abonne' else 'Abonne' end as statut_abonne, 
  (concat(UPPER(lastname),' ',INITCAP(firstname))) as name, 
  role,
  godfather,
  email,
  phone,
  createdat,
  updatedat,
  _sdc_received_at,
  _sdc_sequence,
  comments,
  newsletter,
  last4,
  iat,
  godsons,
  formula
      
from
    {{ ref('src_mongodb_users') }}
order by user_id_mongodb asc 

), 

data_consolidation_stripe as (
  select 
        stripe_customer_id, 
        receipt_email, 
        case when date_diff( current_date(), max(charge_date), day) > 90 then 'Churn' else 'Retain' end as customer_status,         
        count(distinct subscription_id) as subscriptions, 
        count(subscription_id) as subscriptions_occurence, 
        min(charge_date) as first_payment , 
        max(charge_date) as last_payment ,  
        count(case when charge_type = 'Abonnement' then charge_id end ) as total_subscriptions,
        count(case when charge_type = 'Abonnement + Petit Plus' then charge_id end ) as total_petitplus,
        count(case when charge_type = 'Shop' then charge_id end ) as total_Shop,
        count(case when charge_type = 'Abonnement + Parrainage' then charge_id end ) as total_parrainages, 
        count(case when charge_type = 'Abonnement + Coupon' then charge_id end ) as total_coupon, 
        sum(case when charge_type = 'Abonnement' then 	charges_amount end ) as total_ca_subscriptions,
        sum(case when charge_type = 'Abonnement + Petit Plus' then charges_amount end ) as total_petitplus_ca,
        sum(case when charge_type = 'Shop' then charges_amount end ) as total_shop_amount,
        round(avg(case when charge_type = 'Shop' then charges_amount end ),2) as avg_shop_amount,
        --sum(case when charge_type = 'Abonnement + Parrainage' then charge_id) as total_parrainages, 
        --sum(case when charge_type = 'Abonnement + Coupon' then charge_id) as total_coupon, 
        sum(amount_refunded) as amount_refunded

from {{ ref('stg_charges_consolidation') }}
group by 1,2
)

select 
     * from data_consolidation_users 
     left join data_consolidation_stripe
     on data_consolidation_users.email = data_consolidation_stripe.receipt_email 
order by user_id_mongodb asc      




 