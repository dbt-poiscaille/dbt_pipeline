{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'production
    '}  
   )
}}


select
  _id as user_id_mongodb,
  customer as user_id_stripe,
  case when customer is null then 'No StripeId' else 'StripeId'end id_stripe_status,
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