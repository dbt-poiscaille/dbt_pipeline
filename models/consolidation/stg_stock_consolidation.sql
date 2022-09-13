{{
  config(
    materialized = 'table',
    labels = {'type': 'mongp', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

 with stock_data as (

select 
  buyer._id as buyer_id,
  buyer.lastname,
  buyer.firstname,
  buyer.email,
  calibersorter,
  typesorter,
  cost.ttc,
  cost.tax,
  cost.currency,
  cost.ht,
  createdat,
  updatedat,
  item.product.name,
  item.product._id as product_id,
  item.product.type as product_type,
  item.meta.method as method,
  item.meta.caliber,
  item.meta.display.name as item_meta_name,
  item.meta.display.packaging item_meta_packaging,
  item.meta.display.type as item_meta_display
 from {{ ref('src_mongodb_stock') }}

 )

  select * from stock_data
