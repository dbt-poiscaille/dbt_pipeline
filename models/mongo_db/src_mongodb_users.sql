{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
  _id,
  customer,
  formula,
  firstname,
  lastname,
  role,
  godfather,
  email,
  createdat,
  updatedat,
  _sdc_received_at,
  _sdc_sequence,
  phone,
  comments,
  newsletter,
  last4,
  iat,
  godsons
  from {{ source('mongodb', 'user') }}