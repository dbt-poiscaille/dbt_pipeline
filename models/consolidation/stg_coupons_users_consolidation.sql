
{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


with 
  data_cp as (
    SELECT
      in_date,
      in_id,
      in_invoice,
      in_subscription_id,
      in_amount,
      customer,
      description,
      SPLIT(description, ' ')[OFFSET(1)] as coupon_name
    FROM
      {{ ref('src_stripe_invoice_items') }}
    WHERE
      LOWER(description) LIKE '%coupon%'
      order by in_date asc 
  ),

  data_cp_with_src as (
    select
      data_cp.*,
      Type as coupon_source
    from data_cp
    left join {{ ref('src_external_coupon') }} src_cp
    on data_cp.coupon_name = src_cp.coupon
  ),

  cp_source as (
    select distinct
      customer,
      first_value(coupon_source) over (partition by customer order by count_cp_src desc) as coupon_source,
    from (
      select
        customer,
        coupon_source,
        count(coupon_source) as count_cp_src
      from data_cp_with_src
      group by 1,2
    )
  )

select 
  data_cp.customer,
  case
    when coupon_source = 'Remises Stripe' then 'Stripe'
    else coupon_source
  end as coupon_source,
  count(distinct description) as nb_coupons,
  round(sum(in_amount)/100,2) as coupons_amount, 
  SPLIT(max(description), ' ')[OFFSET(1)] as last_coupon,

from data_cp
left join cp_source
on data_cp.customer = cp_source.customer
group by 1,2
order by customer asc 

