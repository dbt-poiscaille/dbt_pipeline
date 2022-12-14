{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

SELECT
 distinct 
  user_id ,
  user_email,
  id ,
  stripe_id,
  createdat,
  subscribed, 
  cast(subscribed as date) as subscription_date_mongo,
  date_diff(current_date(), cast(subscribed as date), day) as subscription_days,
  date_diff(current_date(), cast(subscribed as date), month) as subscription_months,
  date_diff(current_date(), cast(subscribed as date), year) as subscription_year,
  formula,
  price,
  startingat,
  lastingat,
  case when startingat is null then 'Cancelled' else 'Active' end as subscription_status,
  place_id ,
  place_name,
  rate,
  quantity,
  allergies_oysters  ,
  allergies_crustaceans  ,
  allergies_shells ,
  allergies_fishes  ,
  allergies_others ,
  allergies_invalid,
  unsubscribed_reason,
  upcoming_deliveryat,
  upcoming_shippingat,
  updatedat,
  _sdc_sequence
from
   {{ ref('src_mongodb_subscriptions')}}  
