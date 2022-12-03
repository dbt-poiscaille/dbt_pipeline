{{
  config(
    materialized = 'table',
    labels = {'type': 'reporting', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select distinct
    date_add(cast(delivery.shippingat as date), interval 1 day) as sale_date,
    _id as sale_id,
    payment.status as payment_status,
    payment.price.ht as payment_price_ht,
    payment.price.ttc as payment_price_ttc,
    payment.paidprice.ht as payment_paidprice_ht,
    payment.paidprice.ttc as payment_paidprice_ttc,
    payment.refundedprice.ht as payment_refundedprice_ht,
    payment.refundedprice.ttc as payment_refundedprice_ttc,
    payment_refunds.value.nth as payment_refunds_value_nth,
    cast(payment_refunds.value.date as date) as payment_refunds_value_date,
    payment_refunds.value.price.ht as payment_refunds_value_price_ht,
    payment_refunds.value.price.ttc as payment_refunds_value_price_ttc,
    offerings.value._id as offering_value_id,
    offerings.value.count as offering_value_count,
    offerings.value.channel as offering_value_channel,
    offerings.value.name as offering_value_name,
    offerings.value.refundedcount offering_value_refundedcount,
    offerings.value.price.ttc as offering_value_price_ttc,
    offerings.value.price.tax as offering_value_price_tax,
    offerings_value_refundeditem.value.paymentrefundnth
    as offerings_value_refundeditem_paymentrefundnth,
    offerings_value_refundeditem.value.reason as offerings_value_refundeditem_reason,
    offerings_value_refundeditem.value.returnedcount
    as offerings_value_refundeditem_returnedcount,
    offerings_value_refundeditem.value.isreturned
    as offerings_value_refundeditem_isreturned,
    offerings_value_refundeditem.value.price.ht
    as offerings_value_refundeditem_price_ht,
    offerings_value_refundeditem.value.price.ttc
    as offerings_value_refundeditem_price_ttc
from
    {{ source('mongodb', 'sale') }},
    unnest(payment.refunds) as payment_refunds,
    unnest(offerings) as offerings,
    unnest(offerings.value.refundeditems) as offerings_value_refundeditem
order by sale_id asc
