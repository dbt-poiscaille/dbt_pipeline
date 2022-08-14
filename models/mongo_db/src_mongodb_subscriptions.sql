{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

/* Tous les abonnements dont le starting_at sont null sont des abonnements annulés */
select 
  _id as subsrciption_id,
  price,
  place,
  place.name,
  place.createdat,
  place.updatedat,
  place.lat,  
  formula,
  _sdc_batched_at,
  _sdc_extracted_at,
  stripe_id as subsrciption_stripe_id,
  nedb_id,
  allergies.oysters,
  allergies.crustaceans,
  allergies.shells,
  allergies.fishes,
  allergies.others,
  allergies.invalid,
  _sdc_sequence,
  _sdc_received_at,
  startingat,
  late.lastingat,
  late.place.lng,
  late.place.name,
  late.place.createdat,
  late.place.updatedat,
  late.place._id as place_id, 
  updatedat,
  quantity,
  rate,
  createdat,
  _sdc_table_version,
  user.firstname,
  user.lastname,
  user.anonymous,
  user.phone,
  user._id as user_id,
  user.email,
  freedelivery,
  subscribed,
  upcoming.status,
  upcoming.deposit.place._id as deposit_place_id,
  upcoming.deposit.deliveryat,
  upcoming.deposit.code.delivery,
  upcoming.deposit.code.collecting,
  upcoming.deposit.reference,
  place.name,
  place._id as place_id_,
  place.closing.reason,
  place.closing.
FROM
  ,
  place.closing.to,
  coupon
from {{ source('mongodb', 'subscription') }}

