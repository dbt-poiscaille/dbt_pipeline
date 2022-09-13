


{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'production'}  
   )
}}


-- A pousser dans la base contacts

with supplier_data as (
SELECT
  _id as supplier_id,
  name as supplier_name, 
  market.name as supplier_markert_name,
  boat.name as supplier_boat_name,
  carrier as supplier_carrier,
  contract as supplier_contrat,
  boat.licenseplate as supplier_boat_licence,
  methods as supplier_methode, 
  traytype as supplier_traytype,
  location as supplier_location,
  faolocation as supplier_faolocation, 
  methods.value as supplier_methods
FROM  `poiscaille-358510.poiscaille_mongodb.supplier`
left join unnest (methods) methods
order by _id asc   
)

select 
  distinct 
  supplier_id,
  supplier_name,
  supplier_markert_name,
  case when supplier_methods is null then 'Fournisseur' else 'Pêcheur' end as supplier_type,
  supplier_boat_name,
  supplier_carrier,
  supplier_contrat,
  supplier_boat_licence,
  supplier_traytype,
  supplier_location,
  supplier_faolocation
from supplier_data 
order by supplier_id asc 

