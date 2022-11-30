{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with place_data as (
  select 
    *,
    row_number() over (partition by place_email order by place_createdat desc) as rn
  from {{ ref('rep_pr_global_mongo') }}
)

-- get place with most recent created date
select
  *
from place_data
where rn = 1

-- nombre de livraisons en echec (lot2)
-- nombre d'abonnés à 50%
