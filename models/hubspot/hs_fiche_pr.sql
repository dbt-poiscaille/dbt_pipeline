{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select * from {{ ref('rep_pr_global_mongo') }}

-- nombre de livraisons en echec (lot2)
-- nombre d'abonnés à 50%
