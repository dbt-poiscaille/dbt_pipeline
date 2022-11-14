{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}
 

with pr_mongo as ( 
select
  distinct 
  _id as pr_mongo_id,
  name,
  email,
  cast(createdat as date) as create_date,
  phone,
  description,
  details
from
  {{ source('mongodb', 'place') }}
  -- `poiscaille-358510.poiscaille_mongodb.place`
order by 1 asc 
), 

pr_hubspot as ( 
select 
  distinct
  properties.id__kraken_.value as pr_hubspot_kraken_id,
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
  {{ ref('src_hubspot_companies') }}
where 
  property_e_mail_de_l_entreprise is not null
order by 1 asc 
),

pr_mongo_hubspot as (
  select distinct
    pr_hubspot_kraken_id,
    pr_mongo_id,
    pr_hubspot.email_entreprise as pr_in_hubspot,
    pr_mongo.email as pr_in_mongo

    from pr_hubspot
    left join pr_mongo
    on
      pr_mongo.email = pr_hubspot.email_entreprise
)

select
  *,
  IFNULL(pr_in_mongo,pr_in_hubspot) as pr_email,
  case
    when pr_in_hubspot is not null and pr_in_mongo is not null then 'Partenaire'
    when pr_in_hubspot is not null and pr_in_mongo is null then 'Ancien partenaire'
  end as pr_statut_lead,
  case
    when pr_in_hubspot is not null and pr_in_mongo is not null then 'OPEN'
    when pr_in_hubspot is not null and pr_in_mongo is null then 'IN_PROGRESS'
  end as hs_lead_status
  
from pr_mongo_hubspot