{{
  config(
    materialized = 'incremental',
    labels = {'type': 'funnel', 'contains_pie': 'yes', 'category':'source'}  
   )
}}
-- Ne tient compte que des ab
with data_sale as (
select 
  distinct 
  sale_id,
  sale_date,
  email,
  user_id,
from
   {{ ref('stg_mongo_sale_consolidation') }}
   where type_sale='Abonnement'
), 
data_lm as (

  select 
      sale_date,
      email as email_lm, 
      user_id as user_id_lm, 
      from data_sale 
      where data_sale.sale_date between date_trunc(date_sub(current_date(), interval 1 month), month) AND last_day(date_sub(current_date(), interval 1 month), month)
) , 

data_cm as (
     select 
      sale_date,
      email as email_cm, 
      user_id as user_id_cm, 
      from data_sale 
      where data_sale.sale_date between  DATE_TRUNC(current_date(), month) and LAST_DAY(current_date(), month) 
) ,
 data_consolidation as (
select
    data_lm.email_lm, 
    data_lm.user_id_lm, 
    email_cm,
    user_id_cm
    from data_lm
    left join data_cm
    on data_lm.user_id_lm = data_cm.user_id_cm
 ) , 

final_consolidation as (
 select 
 current_date() as date, 
 count( distinct user_id_lm) as users_last_month, 
 count(distinct user_id_cm) as users_current_month 
 from data_consolidation
) 


 select * from final_consolidation
{% if is_incremental() %}
where  date > (select max(date) from {{ this }})
{% endif %}  
order by date desc