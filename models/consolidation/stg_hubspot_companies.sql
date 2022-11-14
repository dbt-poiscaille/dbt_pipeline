{{
  config(
    materialized = 'table',
    labels = {'type': 'hubspot', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with hubspot_companies_data as (
        SELECT distinct
            properties.id__kraken_.value as pr_hubspot_kraken_id,
            property_name.value as pr_name,
            property_address.value as pr_address,
            property_e_mail_de_l_entreprise.value as email_entreprise,  
            property_zip.value as zip,
            property_date_de_mise_en_ligne.value as date_mise_en_ligne,
            property_type.value as pr_type,
            property_typologie_de_pr.value as typology_pr,
            property_createdate.value as pr_createdate,
            property_phone.value as pr_phone,
            properties.country.value as country,
            properties.city.value as city,
            property_hs_lead_status.value as hs_lead_status,
            properties.hs_lastmodifieddate.value as hs_lastmodifieddate,
            -- properties.ca_global.value as ca_global,
            -- properties.ca_p_tits_plus.value as ca_p_tits_plus,
            -- properties.hs_total_deal_value.value as hs_total_deal_value,
            properties.nombre_d_abonnes_associe_au_pr.value as nombre_d_abonnes_associe_au_pr,
            row_number() over (partition by property_e_mail_de_l_entreprise.value order by properties.hs_lastmodifieddate.value desc) as rn

        from {{ ref('src_hubspot_companies') }}
    ),

    lastest_hubspot_comapnies_data as (
        select
            *
        from hubspot_companies_data
        where rn = 1       
    ),

    pr_mongo_data as (
        select distinct
            place_email
        from {{ ref('rep_pr_global_mongo') }}
    ),

    result as (
        select
            *,

            case 
                when email_entreprise in (select place_email from pr_mongo_data) then 'True'
                when email_entreprise not in (select place_email from pr_mongo_data) then 'False'
            end as PR_in_mongo,

            case 
                when email_entreprise in (select place_email from pr_mongo_data) then 'OPEN' -- OPEN = Partenaire
                when email_entreprise not in (select place_email from pr_mongo_data) and hs_lead_status = 'OPEN' then 'IN_PROGRESS' -- IN_PROGRESS = Ancien Partenaire
                when email_entreprise not in (select place_email from pr_mongo_data) and hs_lead_status is null then 'NEW' -- NEW = Prospect
                else hs_lead_status
            end as hs_lead_status_updated
        from lastest_hubspot_comapnies_data
    )

select * from result
-- where PR_in_mongo = 'False'
-- and email_entreprise = 'elise.vitre@mamiemesure.fr'