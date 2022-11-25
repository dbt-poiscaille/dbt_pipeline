{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


select 
   distinct 
  sale_id,
  channel,
  deliveryat,
  shippingat,
  place_id,
  name,
  description,
  TRIM(SPLIT(description, ',')[SAFE_OFFSET(1)]) as city , 
  company,
  delay,
  firstname,
  lastname,
  phone,
  user_id,
  email,
  anonymous,
  createdat,
  updatedat,
  subscription_total_casiers,
  subscription_status,
  subscription_bonus,
  subscription_quantity,
  subscription_rate,
  subscription_price,
  subscription_id,
  margin,
  --margin__st,
  price_ttc,
  price_currency,
  price_ht,
  customerid,
  subscriptionid,
  invoiceitemid,
  chargeid,
  status,
  --ttc,
  currency,
  ht,
  offerings_value_price_ttc,
  offerings_value_price_tax,
  offerings_value_price_currency,
  offerings_value_price_ht,
  offerings_value_count,
  offerings_value_name,
  offerings_value_channel,
  offerings_value_id,
  country,
  offerings_value_items_value_image_id,
  offerings_value_items_value_image_url,
  offerings_value_items_value_publicportionquantity,
  offerings_value_items_value_product_name,
  offerings_value_items_value_product_id,
  offerings_value_items_value_product_type,
  offerings_value_items_value_cost_ttc,
  offerings_value_items_value_cost_unit,
  offerings_value_items_value_cost_tax,
  offerings_value_items_value_cost_currency,
  offerings_value_item_value_cost_ht,
  offerings_value_items_value_cost_tax__it,
  offerings_value_items_value_portion_unit,
  offerings_value_items_value_portion_quantity,
  offerings_value_items_value_piececount,
  offerings_value_items_value_meta_method,
  offerings_value_items_value_meta_caliber,
  offerings_value_items_value_meta_display_plural,
  offerings_value_items_value_meta_display_feminine,
  offerings_value_items_value_meta_display_name,
  offerings_value_items_value_meta_display_packaging,
  offerings_value_items_value_meta_display_type,
  offerings_value_items_value_supplier_carrier,
  offerings_value_items_value_supplier_harbor_name,
  offerings_value_items_value_supplier_name,
  offerings_value_items_value_supplier_id,
  offerings_value_items_value_supplier_boat_name,
  offerings_value_items_value_supplier_url,
  offerings_value_items_value_description
from
  {{ ref('src_mongodb_sale') }}
  where status is null
    or status = 'paid'

