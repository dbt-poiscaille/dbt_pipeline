{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with product_mongo as ( 
select 
  _id as product_id,
  type as product_type,
  latinname as product_latinname,
  faocode as product_faocode,
  name as product_name,
  allergens as product_allergens
from {{ ref('src_mongodb_product') }}
), 

product_stock as (
select
  distinct 
  item.product._id,
  item.product.name,
  --item.description,
  item.product.type, 
  item.meta.method,
  item.meta.display.name as product_preparation,
  item.image.url
  from {{ ref('src_mongodb_stock') }}
)

select 
 * from product_mongo
 left join product_stock
 on product_mongo.product_id = product_stock._id 
