{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

WITH sale_data as (
SELECT
  _id AS sale_id,
  channel,
  delivery.deliveryat,
  delivery.shippingat,
  delivery.place._id as place_id,
  delivery.place.name,
  delivery.place.description,
  delivery.place.shipping.company,
  delivery.place.shipping.delay,
  user.firstname,
  user.lastname,
  user.phone,
  user._id AS user_id,
  user.email,
  user.anonymous,
  createdat,
  updatedat,
  subscription.ref.nth AS subscription_total_casiers,
  subscription.ref.status as subscription_status,
  subscription.ref.bonus as subscription_bonus,
  subscription.quantity as subscription_quantity,
  subscription.rate as subscription_rate,
  subscription.price as subscription_price,
  subscription._id AS subscription_id,
  payment.margin,
  payment.margin__st,
  payment.price.ttc as price_ttc,
  payment.price.currency as price_currency,
  payment.price.ht as price_ht,
  payment.stripe.customerid,
  payment.stripe.subscriptionid,
  payment.stripe.invoiceitemid,
  payment.stripe.chargeid,
  payment.status,
  payment.refundedprice.ttc AS refundedprice,
  payment.refundedprice.currency,
  payment.refundedprice.ht,
  offerings.value.price.ttc AS offerings_value_price_ttc,
  offerings.value.price.tax AS offerings_value_price_tax,
  offerings.value.price.currency AS offerings_value_price_currency,
  offerings.value.price.ht AS offerings_value_price_ht,
  offerings.value.count AS offerings_value_count,
  offerings.value.name AS offerings_value_name,
  offerings.value.channel AS  offerings_value_channel,
  offerings.value._id AS  offerings_value_id,
  offerings.value.items AS offerings_value_items
  --offerings.value.refunded AS  offerings_value_refunded,
 from  {{ source('mongodb', 'sale') }},
 UNNEST(offerings) offerings
) 

select 
*,
'France' as country, 
--offerings_value_items.value.role AS offerings_value_items_value_role,
--TRIM(SPLIT(description, ',')[OFFSET(1)]) as place_city , 
--SUBSTR(TRIM(SPLIT(description, ',')[OFFSET(1)]), -6) as place_info,
offerings_value_items.value.image._id AS offerings_value_items_value_image_id,
offerings_value_items.value.image.url AS offerings_value_items_value_image_url,
offerings_value_items.value.publicportionquantity AS offerings_value_items_value_publicportionquantity,
--offerings_value_items.value.recipes AS offerings_value_items_value_recipes,
offerings_value_items.value.product.name AS offerings_value_items_value_product_name,
offerings_value_items.value.product._id AS offerings_value_items_value_product_id,
offerings_value_items.value.product.type AS offerings_value_items_value_product_type,
--offerings_value_items.value.product.ref AS offerings_value_items_value_product_ref,
offerings_value_items.value.cost.ttc AS offerings_value_items_value_cost_ttc,
offerings_value_items.value.cost.unit AS offerings_value_items_value_cost_unit,
offerings_value_items.value.cost.tax AS offerings_value_items_value_cost_tax,
offerings_value_items.value.cost.currency AS offerings_value_items_value_cost_currency,
offerings_value_items.value.cost.ht AS offerings_value_item_value_cost_ht,
offerings_value_items.value.cost.tax__it AS offerings_value_items_value_cost_tax__it,
offerings_value_items.value.portion.unit AS offerings_value_items_value_portion_unit,
offerings_value_items.value.portion.quantity AS offerings_value_items_value_portion_quantity,
offerings_value_items.value.piececount AS offerings_value_items_value_piececount,
offerings_value_items.value.meta.method AS offerings_value_items_value_meta_method,
offerings_value_items.value.meta.caliber AS offerings_value_items_value_meta_caliber, 
--offerings_value_items.value.meta.display AS offerings_value_items_value_meta_display,
offerings_value_items.value.meta.display.plural AS offerings_value_items_value_meta_display_plural,
offerings_value_items.value.meta.display.feminine AS offerings_value_items_value_meta_display_feminine,
offerings_value_items.value.meta.display.name AS offerings_value_items_value_meta_display_name,
offerings_value_items.value.meta.display.packaging AS offerings_value_items_value_meta_display_packaging,
offerings_value_items.value.meta.display.type AS offerings_value_items_value_meta_display_type,
offerings_value_items.value.supplier.carrier AS offerings_value_items_value_supplier_carrier,
offerings_value_items.value.supplier.harbor.name AS offerings_value_items_value_supplier_harbor_name,
offerings_value_items.value.supplier.name AS offerings_value_items_value_supplier_name ,
offerings_value_items.value.supplier._id AS offerings_value_items_value_supplier_id,
offerings_value_items.value.supplier.boat.name AS offerings_value_items_value_supplier_boat_name,
offerings_value_items.value.supplier.url AS offerings_value_items_value_supplier_url,
offerings_value_items.value.description AS offerings_value_items_value_description , 

  FROM sale_data,
   UNNEST(offerings_value_items) offerings_value_items