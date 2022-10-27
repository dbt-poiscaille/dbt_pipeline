{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with sale_consolidation as (
    select distinct
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
        sale_boutique_ttc,
        sale_locker_ttc,
        sale_bonus_ttc,
        sale_bonus_ttc+sale_boutique_ttc+sale_locker_ttc as sale_total_ttc,

        extract(year from sale_date) as sale_year,
        extract(month from sale_date) as sale_month,
        sale_date,
        prev_sale_date,
        date_diff(sale_date,prev_sale_date,month) as months_from_prev_charge,
        date_diff(sale_date,prev_sale_date,day) as days_from_prev_charge,
        prev_subscription_sale_date,
        prev_shop_sale_date,
        user_transaction_phase,

        place_city,
        nom_departement,
        nom_region,
        type_livraison
    from {{ ref('stg_mongo_sale_consolidation') }}
),

contact_cohort as (
    select distinct
        _id as user_id,
        cast(createdat as date) as contact_createdat
    from {{ ref('src_mongodb_users') }}
)

select
    sale_consolidation.*,
    contact_createdat
from sale_consolidation
left join contact_cohort
on sale_consolidation.user_id = contact_cohort.user_id