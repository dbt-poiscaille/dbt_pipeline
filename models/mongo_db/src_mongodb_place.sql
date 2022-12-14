{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

/* Prendre la dernière version des infos mises à jour pour chaque supplier */

with data_user_mongo as (
select 
  _id,
  description,
  right(description,5) as postalcode,
  lng,
  case when storage ='' then NULL else storage end as storage,
  email,
  case when icebox ='' then NULL else icebox end as icebox,
  name,
  schedule,
  _sdc_table_version,
  createdat,
  updatedat,
  _sdc_received_at,
  _sdc_sequence,
  phone,
  lat,
  shipping.delay,
  shipping.pickup,
  shipping.company,
  openings,
  openings[OFFSET(0)].value.createdat as openings_createdat, 
  openings[OFFSET(0)].value.schedule as openings_schedule, 
  openings[OFFSET(0)].value.hidden as openings_hidden,  
  openings[OFFSET(0)].value.extra as openings_extra, 
  openings[OFFSET(0)].value.day as openings_day, 
  openings[OFFSET(0)].value.tour.depositschedule as openings_depositschedule,
  openings[OFFSET(0)].value.old_id as deposit_oldid,  
  details,
  restricted.quantity,
  restricted.quantity__fl,
  restricted.channel,
  restricted.quantity__it,
  case when depositschedule ='' then NULL else depositschedule end as depositschedule,
  case when dedicated ='' then NULL else dedicated end as dedicated,
  extraopenings, 
  shipping,
  --extraopenings.value.date,
  --extraopenings.value.schedule,
  --extraopenings.value.createdat,
  --extraopenings.value.extra,
  --extraopenings.value.hidden,
  case when comment ='' then NULL else comment end as comment,
    _sdc_batched_at,
  _sdc_extracted_at,
  closing.reason as closing_raison,
  closing.to as closing_to,
  closing.from as closing_from
--  closing[OFFSET(0)].value.reason as closing_raison, 
--  closing.to as closing_to, 
--  closing.frm as closing_from, 
from 
   {{ source('mongodb', 'place') }}
order by name asc 
)

select 
     *,
      case
        when delay = 1 and openings_day = 'Monday' then 'Mardi'
        when delay = 1 and openings_day = 'Tuesday' then 'Mercredi' 
        when delay = 1 and openings_day = 'Wednesday' then 'Jeudi' 
        when delay = 1 and openings_day = 'Thursday' then 'Vendredi'
        when delay = 1 and openings_day = 'Friday' then 'Mardi' 
        when delay = 1 and openings_day = 'Saturday' then 'Mardi' 

        when delay = 0 and openings_day = 'Monday' then 'Mardi'
        when delay = 0 and openings_day = 'Tuesday' then 'Mardi' 
        when delay = 0 and openings_day = 'Wednesday' then 'Mercredi' 
        when delay = 0 and openings_day = 'Thursday' then 'Jeudi' 
        when delay = 0 and openings_day = 'Friday' then 'Vendredi' 
        when delay = 0 and openings_day = 'Saturday' then 'Samedi' 
        end as openings_day_fr
    from data_user_mongo      




