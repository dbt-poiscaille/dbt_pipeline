{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select
  _id,
  latinname,
  allergens,
  --allergens[OFFSET(0)].value as openings_createdat, 
  faocode,
  name,
  _sdc_table_version,
  createdat,
  updatedat,
  _sdc_received_at,
  _sdc_sequence,
  type,
  _sdc_batched_at,
  _sdc_extracted_at
from {{ source('mongodb', 'product') }}

order by _id asc 