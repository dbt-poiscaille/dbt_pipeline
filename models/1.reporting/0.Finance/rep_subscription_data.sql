{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with subscription_raw_data as (
    select
        user_id ,
        user_email,
        id ,
        stripe_id,
        createdat,
        subscribed, 
        subscription_date_mongo,
        formula,
        price,
        startingat,
        lastingat,
        subscription_status,
        place_id,
        place_name,
        rate,
        quantity,
        unsubscribed_reason,
        date(upcoming_shippingat,'Europe/Paris') as upcoming_shippingat,
        date(upcoming_deliveryat, 'Europe/Paris') as upcoming_deliveryat,
        updatedat,
        _sdc_sequence,
        row_number() over (partition by id order by _sdc_sequence desc) as rn
    from {{ ref('stg_subscription_consolidation') }}
)

select
    *
from subscription_raw_data
where rn = 1