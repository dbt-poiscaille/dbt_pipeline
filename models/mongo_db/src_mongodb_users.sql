{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with user_data as (
select
  distinct 
  _id,
  customer,
  formula,
  firstname,
  lastname,
  role,
  godfather,
  email,
  cast(createdat as date) as createdat,
  phone,
  case when phone like '01%' 
             or phone like '02%'
             or phone like '03%'
             or phone like '04%'
             or phone like '05%'
             or phone like '08%'
             or phone like '09%'       
       then phone end as phone_fixe , 
  case when phone not like '01%' 
             or phone not like '02%'
             or phone not like '03%'
             or phone not like '04%'
             or phone not like '05%'
             or phone not like '08%'
             or phone not like '09%'       
       then phone end as phone_mobile , 
  comments,
  newsletter,
  last4,
  iat,
  godsons.value as godson 
  from {{ source('mongodb', 'user') }},
  unnest (godsons) godsons
)

select 
      distinct 
     *, 
     concat('+33',substr(cast(phone_fixe as string),2,10)) as phone_fixe_f ,
    concat('+33',substr(cast(phone_mobile as string),2,10)) as phone_mobile_f ,
    from user_data
    order by _id asc 