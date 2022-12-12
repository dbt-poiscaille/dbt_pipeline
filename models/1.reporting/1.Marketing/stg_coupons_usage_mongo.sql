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
  user._id as user_id,
  user.email as user_email,
  user.firstname as user_firstname,
  user.lastname as user_lastname,
  coupon._id as coupon_id,
  coupon.type as coupon_type,
  coupon.channel as coupon_channel,
  coupon.code as coupon_code,
  coupon.freedelivery as coupon_freedelivery,
  coupon.message as coupon_message,
  cast(createdat as date) as createdat,
  updatedat, 
  RANK() OVER ( PARTITION BY  user._id ORDER BY cast(updatedat as date) desc  ) AS rank
FROM
 {{ source('mongodb', 'coupon_usage') }} 
)

select 
  
 * from data_consolidation
 where rank = 1 