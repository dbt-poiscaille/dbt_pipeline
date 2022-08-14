{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


select 
  distinct  
  _id AS pr_id,
  case when INITCAP(details) =''  then 'No Name' else INITCAP(details) end as pr_name, 
  storage , 
  icebox, 
  schedule, 
  description AS adresse,
  postalcode,
  openings_createdat,
  openings_schedule,
  openings_extra,
  openings_day,
  openings_depositschedule as openings_deposit_schedule,
  phone, 
  email, 
  concat ('(',lat,',',lng,')') as latitude_longitude,
  case when closing_raison = 'noSchedule' then 'Ferme' else 'Ouvert' end Statut , 
  cast(closing_to as date) as Ferme_du,
  cast(closing_from as date) as Ferme_au
FROM
    {{ ref('src_mongodb_place') }}
order by pr_id asc   