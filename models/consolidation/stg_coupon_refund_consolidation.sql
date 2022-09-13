
{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe_mongodb', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

with coupon_data as ( 
select 
      in_date, 
      nb_customer_cp, 
      abs(total_amount_cp) as total_amount_cp
from {{ ref('stg_coupons_consolidation') }}
), 

refund_data as (
select 
  charge_date,
  round(sum(amount_refunded)/100,2) as amount_refunded
from 
 {{ ref('stg_charges_consolidation') }}
group by 1 
)

select 
    * from coupon_data
    left join refund_data
    on coupon_data.in_date = refund_data.charge_date
