{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with sale_consolidation as (
    select
        *
    from {{ ref('stg_mongo_sale_cleanup') }}
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