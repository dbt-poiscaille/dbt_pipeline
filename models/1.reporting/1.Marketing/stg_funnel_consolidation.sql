{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with 
sale_consolidation as (
select * from {{ ref('stg_funnel_avg_order') }}
), 
customers as (
    select * from {{ ref('stg_funnel_customers_type') }}
),
visiteurs_ga as (
    select * from {{ ref('stg_funnel_visiteur') }}
), 
returning_customers as (
    select * from {{ ref('stg_funnel_returning_customers') }}
)

select 
     * from sale_consolidation
     left join customers
     on sale_consolidation.sale_data_order = customers.sale_date    
     left join visiteurs_ga
     on sale_consolidation.sale_data_order = visiteurs_ga.event_date 
     left join returning_customers
     on sale_consolidation.sale_data_order = returning_customers.date
