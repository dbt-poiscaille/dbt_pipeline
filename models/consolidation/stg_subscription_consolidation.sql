{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


with mongo_data as (
SELECT
  user_id ,
  user_email,
  id ,
  stripe_id,
  cast(subscribed as date) as subscription_date_mongo,
  date_diff(current_date(), cast(subscribed as date), day) as subscription_days,
  date_diff(current_date(), cast(subscribed as date), month) as subscription_months,
  date_diff(current_date(), cast(subscribed as date), year) as subscription_year,
  formula,
  price,
  startingat,
  case when startingat is null then 'Cancelled' else 'Active' end as subsription_status,
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
from
   {{ ref('src_mongodb_subscriptions')}}  
),

stripe_data as ( 

  SELECT
  distinct 
  id as subscription_id_stripe,
  customer as customer_stripe, 
  cast (start_date as date) as subscription_date_stripe,
  plan_id as subscription_type_stripe,
  plan_interval as subscription_intervall_stripe,
  plan_interval_count as subscription_interval_count_stripe,
  plan_amount as subscription_price_stripe,
  plan_name as subscription_name_stripe
from
 {{ ref('src_stripe_subscriptions')}}  
where plan_name is not null 
) , 

place_data as ( 

SELECT
  _id,
  description,
  postalcode,
  lng,
  storage,
  email as place_email,
  icebox,
  schedule,
  openings_schedule,
  openings_day
from
{{ ref('src_mongodb_place')}}  
)


select *
   from mongo_data
   left join stripe_data
   on mongo_data.stripe_id = stripe_data.subscription_id_stripe
   left join place_data
   on mongo_data.place_id = place_data._id
   where mongo_data.user_id is not null /* Retirer tous les id utilisateurs null */





