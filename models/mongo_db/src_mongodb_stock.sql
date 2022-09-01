{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select
 distinct 
  _id AS stock_id,
  item.product.name AS product_name,
  item.product._id AS product_id,
  item.product.type,
  item.product.ref.transformed,
  buyer.firstname,
  buyer.lastname,
  buyer._id AS buyer_id,
  buyer.email,
  calibersorter,
  calibersorter__st,
  typesorter,
  excludedfrompotential,
  cost.ttc,
  cost.tax,
  cost.currency,
  cost.ht,
  _sdc_table_version,
  item.image._id AS image_id,
  item.image.url as item_image_url,
  item.supplier.carrier,
  item.supplier.name AS supplier_name,
  item.supplier._id AS supplier_id,
  item.supplier.url,
  item.supplier.boat.name AS boat_name,
  item.description,
  createdat,
  arrivalpart,
  updatedat,
from 
 {{ source('mongodb', 'stock') }}