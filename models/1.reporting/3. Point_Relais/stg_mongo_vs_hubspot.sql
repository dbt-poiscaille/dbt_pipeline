

{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel', 'contains_pie': 'no', 'category':'source'}  
   )
}}
 

with pr_mongo as ( 
select
  distinct 
  _id,
  name,
  email,
  cast(createdat as date) as create_date,
  phone,
  description,
  details
from 
  `poiscaille-358510.poiscaille_mongodb.place`
order by 1 asc 
), 

pr_hubspot as ( 
select 
  distinct 
  property_name.value as pr_name,
  property_address.value as pr_address,
  property_e_mail_de_l_entreprise.value as email_entreprise,  
  property_zip.value as zip,
  property_date_de_mise_en_ligne.value as date_mise_en_ligne,
  property_type.value as pr_type,
  property_typologie_de_pr.value as typology_pr,
  property_createdate.value as pr_createdate,
  property_phone.value as pr_phone   
FROM
  `poiscaille-358510.poiscaille_hubspot.companies`
where property_e_mail_de_l_entreprise is not null 
order by 1 asc 
)

select 
   distinct 
   pr_mongo.email as pr_in_mongo,
   pr_hubspot.email_entreprise as pr_in_hubspot

   from pr_mongo
   left join pr_hubspot
   on pr_mongo.email = pr_hubspot.email_entreprise














