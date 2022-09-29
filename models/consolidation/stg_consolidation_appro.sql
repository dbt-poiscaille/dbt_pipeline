  {{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_sale as (

SELECT
  shippingat,
  sale_id,
  first_day_week,
  last_day_week,
  offerings_value_price_ttc,
  offerings_value_price_ht,
  offerings_value_price_tax,
  offerings_value_channel,
  channel,
  status,
  customerid,
  price_ht,
  price_ttc,
  subscription_id,
  subscription_status,
  subscription_total_casiers,
  createdat,
  updatedat,

FROM  {{ ref('src_mongodb_sale') }}
   where channel ='combo'
  order by shippingat desc 
)

select 
       shippingat , 
       first_day_week,
       last_day_week,
       count (distinct sale_id) as total_casiers,
       count (distinct case when subscription_status in ('chosen/pre', 'chosen/self', 'forced/self') then sale_id end) as  choix_client  ,
       count (distinct case when subscription_status not in ('chosen/pre', 'chosen/self', 'forced/self') then sale_id end) as  hors_choix_client  ,
       from data_sale 
       group by 1,2,3