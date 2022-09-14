{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

select 
* 
from {{ ref('rep_clients_kpi_mongo') }}
order by user_id asc 


-- données à partir de la table sale ( start Juillet 2022)
-- CLV 