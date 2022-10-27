{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with sale_data as (
    select
        sale_id,
        user_id,
        firstname,
        lastname,
        email,
        subscription_id,
        customerid,
        subscription_rate,
        subscription_status,
        subscription_type,
        type_sale,
        price_ttc,

        extract(year from sale_date) as sale_year,
        extract(month from sale_date) as sale_month,
        sale_date,
        prev_sale_date,
        prev_subscription_sale_date,
        prev_shop_sale_date,
        user_transaction_phase,

        place_city,
        nom_departement,
        nom_region,
        type_livraison
    from {{ ref('stg_mongo_sale_consolidation') }}
),

final_sale_data as (
    select
        *,
        date_diff(sale_date,prev_sale_date,month) as months_from_prev_charge,
        date_diff(sale_date,prev_sale_date,day) as days_from_prev_charge,
    from sale_data
),

sale_month_data as (
    select
        sale_year,
        sale_month,

        count(distinct customerid) as total_customer_in_month,
        count(distinct case when months_from_prev_charge is null then customerid end) as nb_new_customer,
        count(distinct case when months_from_prev_charge = 1 then customerid end) as nb_customer_from_month_n_1,
        count(distinct case when months_from_prev_charge = 2 then customerid end) as nb_customer_from_month_n_2,
        count(distinct case when months_from_prev_charge = 3 then customerid end) as nb_customer_from_month_n_3,


        count(distinct sale_id) as total_number_sale_ttc,
        round(count(distinct sale_id)/count(distinct customerid),2) as avg_monthly_purchase_freq,
        round(sum(price_ttc),2) as revenue_ttc, 
        round(sum(price_ttc)/count(distinct customerid),2) as avg_revenue_ttc_per_customer

    from final_sale_data
    group by 1,2
),

sale_retention_data as (
    select 
        *,
        LAG(total_customer_in_month) OVER (ORDER BY sale_year ASC, sale_month ASC) as total_customer_in_month_n_1,
        round(safe_divide(nb_customer_from_month_n_1,LAG(total_customer_in_month) OVER (ORDER BY sale_year ASC, sale_month ASC)),2) as retention_rate,
        round(safe_divide(1,1-safe_divide(nb_customer_from_month_n_1, LAG(total_customer_in_month) OVER (ORDER BY sale_year ASC, sale_month ASC))),2) as avg_lifetime_in_months,
    from sale_month_data    
),

result as (
    select
        *,
        round(avg_revenue_ttc_per_customer*avg_lifetime_in_months,2) as avg_customer_lifetime_value_ttc
    from sale_retention_data
)

select
*
from result
order by sale_year desc, sale_month desc
