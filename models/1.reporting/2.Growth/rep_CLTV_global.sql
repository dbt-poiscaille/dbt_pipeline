{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with month_data as (
  SELECT  
    charge_year,
    charge_month,
    count(distinct customer) as total_customer_in_month,
    count(distinct case when months_from_prev_charge is null then customer end) as nb_new_customer,
    count(distinct case when months_from_prev_charge <= 1 then customer end) as nb_customer_retained_in_month,
    round(count(distinct case when months_from_prev_charge <= 1 then customer end)/count(distinct customer),2) as retention_rate,
    count(distinct charge_id) as total_nb_charge,
    round(count(distinct charge_id)/count(distinct customer),2) as avg_monthly_purchase_freq,
    round(sum(charges_amount/100),2) as revenue_ttc, 
    round(sum(charges_amount/100)/count(distinct customer),2) as avg_revenue_ttc_per_customer

  FROM {{ ref('rep_stripe_retention') }} 
  group by 1,2
),
month_retention_data as (
  select
    *,
    round(1/(1-nb_customer_retained_in_month/total_customer_in_month),2) as avg_customer_lifetime
  from month_data
),

result as (
  select
    *,
    round(avg_monthly_purchase_freq*avg_revenue_ttc_per_customer*avg_customer_lifetime,2) as avg_customer_lifetime_value_ttc
    
  from month_retention_data
  order by charge_year desc, charge_month desc
)

select * from result
