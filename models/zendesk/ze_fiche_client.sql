{{
  config(
    materialized = 'table',
    labels = {'type': 'zendesk', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select 
 distinct 
  user_status,
  link_kraken,
  user_id,
  total_ca_global,
  ca_global,
  pan_moy,
  subscription_date,
  subscription_type,
  subscription_status,
  last_payment,
  place_openings_day_livraison,
  place_openings_day_preparation,
  localisation,
  place_name,
  place_address,
  place_city,
  place_codepostal,
  place_openings_day,
  place_type,
  nb_casiers,
  allergies_crustaceans,
  allergies_shells,
  allergies_fishes,
  allergies_others,
  allergies_invalid,
  -- info allergie à ajouter
  user_id_subscription,
  customer_id_stripe,
from {{ ref('rep_clients_kpi_mongo') }} 
where 
  user_status != 'lead'
order by user_id asc 




-- données à partir de la table sale ( start Juillet 2022)
-- CLV 