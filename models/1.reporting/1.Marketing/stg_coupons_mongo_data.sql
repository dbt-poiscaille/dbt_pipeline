{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'yes',
            'category': 'source',
        },
    )
}}
with data_consolidation as (
select
    _id as sale_id ,
    user._id as user_id,
    cast(delivery.shippingat as date) as shipping_date,
    DATE_ADD(cast(delivery.shippingat as date), INTERVAL 1 DAY) as sale_date,
    channel ,
    payment.price.ttc as payment_price_ttc,
    payment.status as payment_status,
    payment.coupon.discountpercentage as payment_coupon_discount,
    payment.coupon.type as payment_coupon_type,
    payment.coupon.combodiscountedcount as payment_coupon_combo,
    payment.coupon._id as payment_coupon_id,
    payment.coupon.channel as payment_coupon_channel,
    payment.coupon.code as payment_coupon_code,
    payment.coupon.discountprice.ttc as payment_coupon_discountprice
from  {{ source('mongodb', 'sale') }}
order by cast(delivery.shippingat as date) desc
)

select 
  distinct 
  sale_date, 
  sale_id, 
  shipping_date, 
  user_id, 
  channel, 
  payment_price_ttc, 
  payment_status, 
  payment_coupon_discount, 
  payment_coupon_combo, 
  payment_coupon_type, 
  payment_coupon_code, 
  payment_coupon_id, 
  case when payment_coupon_id is null then 'SANS COUPON' else 'AVEC COUPON' end as coupon_usage,
  payment_coupon_channel, 
  payment_coupon_discountprice
  from data_consolidation
  order by sale_date desc


