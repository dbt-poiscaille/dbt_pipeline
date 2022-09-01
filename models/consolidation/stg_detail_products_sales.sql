{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select 
  cast( createdat as date) as date,
  createdat,
  updatedat,
  email,
  user_id,
  phone,
  lastname,
  company,
  description,
  place_id,
  sale_id,
  subscription_total_casiers,
  subscription_status,
  subscription_bonus,
  subscription_quantity,
  subscription_rate,
  subscription_price,
  subscription_id,
  margin,
  price_ttc,
  price_currency,
  customerid,
  subscriptionid,
  invoiceitemid,
  chargeid,
  status,
  offerings_value_items_value_product_name,
  offerings_value_items_value_product_id,
  offerings_value_items_value_product_type,
  offerings_value_items_value_cost_ttc,
  offerings_value_items_value_cost_unit,
  offerings_value_items_value_cost_tax,
  offerings_value_items_value_cost_tax__it,
  offerings_value_items_value_portion_quantity,
  offerings_value_items_value_meta_display_name,
  offerings_value_items_value_meta_display_packaging,
  offerings_value_items_value_meta_display_type,
  offerings_value_items_value_supplier_carrier,
  offerings_value_items_value_supplier_harbor_name,
  offerings_value_items_value_supplier_name,
  offerings_value_items_value_supplier_id,
  offerings_value_items_value_supplier_boat_name
from
  {{ ref('src_mongodb_sale') }}
order by subscription_id asc 
