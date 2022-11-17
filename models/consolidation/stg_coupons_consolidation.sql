
{{
    config(
        materialized="table",
        labels={"type": "stripe", "contains_pie": "yes", "category": "source"},
    )
}}


with
    data_cp as (
        select
            in_date,
            in_id,
            in_invoice,
            in_subscription_id,
            in_amount,
            customer,
            description,
        from {{ ref("src_stripe_invoice_items") }}
        where lower(description) like '%coupon%'
    ),

    consolidation as (
        select
            in_date,
            description as cp_name,
            count(distinct customer) as nb_customer,
            round(sum(in_amount) / 100, 2) as total_amount
        from data_cp
        group by 1, 2
        order by total_amount desc
    )

select
    in_date,
    sum(nb_customer) as nb_customer_cp,
    round(sum(total_amount), 2) as total_amount_cp
from consolidation
group by 1
order by in_date desc
