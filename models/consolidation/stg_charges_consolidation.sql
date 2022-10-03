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
  place_id,
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
  place_id,
  place_name,
  place_createdat,
  place_updatedat,
  place_description
  FROM subscription
  WHERE rn = 1
),
charges_with_subscription_and_invoice AS (
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
      refunded,
      captured,   
  customer as stripe_customer_id,
  subscription_id,
  stripe_id,
  receipt_email,
  receipt_number,
  charges_invoice,
  type, 
  invoice_subscription,
  place_id,
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
),
charges_with_subscription_and_invoice_detailed AS (
SELECT *,
case when charge_type ='Abonnement + Petit Plus' then 
[STRUCT('Abonnement' as charge_type_details,  CAST(subscription_price AS NUMERIC)  as amount_details), STRUCT('PetitPlus' as charge_type_details, CAST(ptit_plus_amount AS NUMERIC) as amount_details)] 
when  charge_type = 'Abonnement + Coupon' then 
[STRUCT('Abonnement + Coupon' as charge_type_details, CAST(charges_amount AS NUMERIC) as amount_details)]
when  charge_type = 'Abonnement + Parrainage' then 
[STRUCT('Abonnement + Parrainage' as charge_type_details, CAST(charges_amount AS NUMERIC) as amount_details)] 
when  charge_type = 'Abonnement' then 
[STRUCT('Abonnement' as charge_type_details, CAST(charges_amount AS NUMERIC) as amount_details)]
when  charge_type = 'Shop' then 
[STRUCT('Shop' as charge_type_details, CAST(charges_amount AS NUMERIC) as amount_details)]
when  charge_type = 'Other' then 
[STRUCT('Other' as charge_type_details, CAST(charges_amount AS NUMERIC) as amount_details)]
END AS charge_details
FROM charges_with_subscription_and_invoice
ORDER BY charge_id
) , 

region as ( 

SELECT code_postal,
CASE WHEN nom_region IS NOT NULL THEN nom_region
ELSE nom_commune END AS nom_region
FROM  {{ source('external', 'communes_departement_region') }} b
UNION ALL
SELECT '95228','Île-de-France'
ORDER BY code_postal ASC 

), 

place_info as ( 
SELECT
  _id,
  name,
  lat,
  lng
FROM {{ ref('src_mongodb_place')}}  
)


SELECT 
 distinct 
charge_date,
  charge_id, 
  description,
  plan_product,
  charge_details.charge_type_details AS charge_type,
  round(charge_details.amount_details/100,2) AS charges_amount,
  stripe_customer_id, 
        refunded,
      captured,   
  round(amount_refunded/100,2) AS amount_refunded, 
  subscription_id,
  stripe_id,
  receipt_email,
  receipt_number,
  charges_invoice,
  type, 
  invoice_subscription,
  place_id as pr_id,
  place_name as pr_name,
  --place_createdat as pr_creat,
  --place_updatedat,
  place_description as pr_adresse,
  postal_code as pr_code_postal,
  --region.code_postal as region_code_postal, 
  region.nom_region  as pr_region, 
  'France' as pr_country, 
  place_info.lat, 
  place_info.lng, 
  TRIM(SPLIT(place_description, ',')[OFFSET(1)]) as city , 
  SUBSTR(TRIM(SPLIT(place_description, ',')[OFFSET(1)]), -6) as info

FROM 
charges_with_subscription_and_invoice_detailed, 
UNNEST(charge_details) charge_details
left join region
on charges_with_subscription_and_invoice_detailed.postal_code = region.code_postal
left join place_info 
on charges_with_subscription_and_invoice_detailed.place_id = place_info._id
order by charge_date desc 








