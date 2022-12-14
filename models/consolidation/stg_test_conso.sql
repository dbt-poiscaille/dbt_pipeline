{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


WITH charges AS (
SELECT
  distinct 
  id as charge_id,
  receipt_email,
  balance_transaction,
  created,
  payment_intent,
  receipt_number,
  paid,
  invoice AS charges_invoice,
  currency,
  payment_method,
  --receipt_url,
  failure_code,
  failure_message,
  status,
  refunded,
  captured,
  source_exp_year,
  source_object,
  source_country,
  source_id,
  source_name,
  source_customer,
  source_address_country,
  amount AS charges_amount,
  amount_refunded,
  disputed,
  updated,
  customer,
  dispute,
  shipping_address_country,
  shipping_address_city,
  shipping_address_state,
  shipping_address_postal_code,
 ROW_NUMBER() OVER(PARTITION BY id ORDER BY _sdc_extracted_at DESC) AS rn 
 FROM {{ ref('src_stripe_charges')}}  
WHERE  status = 'succeeded' ),
final_charges AS (
SELECT 
      created,
      charges_amount,
      amount_refunded,
      charges.charge_id, 
      receipt_email,
      balance_transaction,
      payment_intent,
      receipt_number,
      customer,
      paid,
      charges_invoice,
      currency,
      payment_method,
     -- receipt_url,
      failure_code,
      failure_message,
      status,
      refunded,
      captured,       
FROM charges 
WHERE rn=1),
invoice_line_items AS (
 SELECT invoice,type,subscription AS invoice_subscription, plan_product ,description,
 ROW_NUMBER() OVER(PARTITION BY invoice ORDER BY _sdc_extracted_at DESC) AS rn
  FROM {{ ref('src_stripe_invoice_line_items')}}  
),
invoice_line_items_distinct AS (
SELECT  invoice,type, invoice_subscription, plan_product,description FROM invoice_line_items WHERE rn = 1),
subscription as (
 SELECT
  id,
  stripe_id, 
  rate,
  quantity,
  startingat,
  price,
  formula,
  place_name,
  place_createdat,
  place_updatedat,
  --place.opening.schedule as place_opening_schedule,
  --place.opening.day as place_opening_days,
  --place.opening.extra as place_opening_extra,
  place_description,
  ROW_NUMBER() OVER(PARTITION BY id ORDER BY updatedat DESC) AS rn 
FROM
 {{ ref('src_mongodb_subscriptions')}}  
), 
subscription_distinct AS (
SELECT
  id AS subscription_id,
  stripe_id, 
  rate,
  quantity,
  startingat,
  price,
  formula,
  place_name,
  place_createdat,
  place_updatedat,
  place_description
  FROM subscription
  WHERE rn = 1
)
SELECT 
  distinct
  cast(created as date) as charge_date,
  charge_id, 
  description,
  plan_product,
  case 
    when description like '%tit plus%' then 'Abonnement + Petit Plus'
    when lower(description) like '%coupon%' then 'Abonnement + Coupon'
    when lower(description) like '%parrainage%' then 'Abonnement + Parrainage'
    when type  = 'subscription' or lower(description) like '%casier%' then 'Abonnement'
    when type is null then 'Shop'
    else 'Other'
     end as charge_type,
  case when description like '%tit plus%' then charges_amount - subscription_distinct.price else 0 end as ptit_plus_amount,
  case when type is null then charges_amount else 0 end as shop_amount, 
  charges_amount, 
  subscription_distinct.price as subscription_price,
  amount_refunded,    
  customer as stripe_customer_id,
  subscription_id
  stripe_id,
  receipt_email,
  receipt_number,
  charges_invoice,
  type, 
  invoice_subscription,
  place_name,
  place_createdat,
  place_updatedat,
  place_description,
  right(place_description,5) as postal_code
    from final_charges
    left join invoice_line_items_distinct 
  on final_charges.charges_invoice = invoice_line_items_distinct.invoice
   left join  subscription_distinct
  on invoice_line_items_distinct.invoice_subscription = subscription_distinct.stripe_id
--where receipt_email = 'zuzannakierepka@hotmail.fr'
  order by charge_id asc


  