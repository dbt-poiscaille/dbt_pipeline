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

SELECT
  distinct 
  cast(createdat as date) as date,
   updatedat,
  _id as sale_id,
  coupon.discountpercentage as coupon_discountpercentage,
  coupon.combodiscountedcount as coupon_combodiscountedcount ,
  coupon.type as coupon_type,
  coupon._id as coupon_id,
  coupon.freedelivery,
  coupon.code,
  payment.price.ttc,
  payment.price.ttc/100 as price_ttc,
  payment.paidprice,
  payment.paidprice/100 as paidprice_100,
   RANK() OVER ( PARTITION BY  _id ORDER BY cast(updatedat as date) desc  ) AS rank
FROM
  {{ source('mongodb', 'gift_cards') }}
  order by _id 
)

select 
  * 
  from data_consolidation
  where rank = 1