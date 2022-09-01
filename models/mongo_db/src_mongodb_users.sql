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
  _sdc_sequence,
  phone,
  comments,
  newsletter,
  last4,
  iat,
  --godsons.value as godsons
  from {{ source('mongodb', 'user') }}