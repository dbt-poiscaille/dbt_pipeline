{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

 with all_data as (
SELECT
  charge_date,
  stripe_customer_id,
  round((cast(charges_amount as int64)/100),2)  as amount,
  charge_type,
  pr_region as nom_region
 from {{ ref('stg_charges_consolidation') }}

  where stripe_customer_id is not null 
order by stripe_customer_id asc , charge_date asc 
      ) ,  

     new_vs_old as (
       select *,
           rank() over (partition by stripe_customer_id order by charge_date) as player_sequence
          from all_data 
       order by stripe_customer_id asc 
       )

    select 
          charge_date , 
          stripe_customer_id,
          amount,  
          player_sequence,
          charge_type, 
          nom_region,
          case when player_sequence = 1 then 'new_customer' else 'returning_customer' end as player_type
     from new_vs_old   
    