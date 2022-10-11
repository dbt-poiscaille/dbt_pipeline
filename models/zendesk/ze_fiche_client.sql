{{
  config(
    materialized = 'table',
    labels = {'type': 'zendesk', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select 
 distinct 
  link_kraken,
  user_status_ as user_status,
  case when email like '%@poiscaille%' then name_zendesk else name end as name , 
  phone_mobil,  
  user_id,
  email, 
  case when total_ca_global is null then 0 else total_ca_global end as total_ca_global,
  case when ca_global is null then 0 else ca_global end as ca_global,
  case when pan_moy is null then 0 else pan_moy end as pan_moy,
  subscription_date,
  subscription_type,
  case when subscription_status is null or subscription_status = 'Cancelled' then false else true end as subscription_status,
  last_payment,
  place_openings_day as place_openings_day_livraison,
  place_openings_day_preparation
  place_openings_schedule,
  localisation,
  place_name,
  place_address,
  place_city,
  place_codepostal,
  place_openings_day,
  place_type,
  case when nb_casiers is null then 0 else nb_casiers end as nb_casiers,
  allergies_crustaceans,
  allergies_shells,
  allergies_fishes,
  case when allergies_shells = false and allergies_crustaceans = false and allergies_fishes = false then false 
       when allergies_shells is null and allergies_crustaceans is null and allergies_fishes is null then false 
  else true end as allergie_exist,
  case when allergies_others is null then false 
       when length(allergies_others) = 0 then false 
       else true end as allergies_others,
  allergies_invalid,
  case when newsletter is null then false else true end as newsletter,
  case when refund_global_stripe is null then 0 else refund_global_stripe end as refund_global_stripe,
  case when user_id_subscription is null then 'No Subscription' else user_id_subscription end as user_id_subscription,
  customer_id_stripe,
from {{ ref('rep_clients_kpi_mongo') }} 
--where user_status != 'lead' 
order by user_id asc 




-- données à partir de la table sale ( start Juillet 2022)
-- CLV 