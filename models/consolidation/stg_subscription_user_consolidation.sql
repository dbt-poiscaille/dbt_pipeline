
{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select  
      user_id, 
      email, 
      max(subscription_date_mongo) as subscription_date, 
      max(subscription_days) as subscription_days, 
      max(subscription_months) as subscription_months, 
      max(subscription_year) as subscription_year,       
      count (distinct subscription_id) as total_subscriptions,  
      count (distinct place_id) as nb_delivery_place, 
      count(distinct case when formula = 'subscription' then subscription_id end ) as nb_subscription, 
      count(distinct case when formula = 'uniq' then subscription_id end ) as nb_uniq, 
      count(distinct case when subsription_status = 'Active' then subscription_id end ) as active_subscription,
      count(distinct case when subsription_status = 'Cancelled' then subscription_id end ) as cancelled_subscription, 
      count(distinct case when pickup = 'relay' then subscription_id end ) as pickup_relay, 
      count(distinct case when pickup = 'home' then subscription_id end ) as pickup_home,
      count(distinct case when company = 'self' then subscription_id end ) as company_self,
      count(distinct case when company = 'chrono' then subscription_id end ) as company_chrono,
      max(oysters) as oysters,
      max(crustaceans) as crustaceans,
      max(shells) as shells,
      max(fishes) as fishes,
      max(others) as others,
      max(postalcode) as postalcode,
      max(name) as place_name, 
      max(description) as place_address,
      max(openings_schedule) as openings_schedule, 
      max(openings_day) as openings_day

   from {{ ref('stg_subscription_consolidation') }}
   -- where user_id='5ee60116895fb442ebeaeed4'
   group by 1 