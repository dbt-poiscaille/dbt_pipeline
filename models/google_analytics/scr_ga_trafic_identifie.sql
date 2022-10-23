{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}


with ga_data as (

select 
    PARSE_DATE('%Y%m%d',event_date) as event_date, 
    event_name,
    case when event_name in ('signUp', 'signIn','signInCheck') then 'connected' else 'not connected' end as identification_status,
    device.category as device_category,
    user_pseudo_id,
    user_id, 
    lower((select value.string_value from unnest(event_params) where key = 'medium')) as medium,
    lower((select value.string_value from unnest(event_params) where key = 'campaign')) as campaign,
    lower((select value.string_value from unnest(event_params) where key = 'source')) as source,       
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
    (select value.string_value from unnest(event_params) where key = 'user_type') as user_type,
    (select value.string_value from unnest(event_params) where key = 'session_engaged') as session_engaged,
    traffic_source.name as traffic_name, 
    traffic_source.source as traffic_source, 
    traffic_source.medium as traffic_medium, 

  from {{ ref('scr_ga_global_data') }}

)

select 
    event_date, 
    device_category, 
    traffic_name, 
    traffic_source,
    traffic_medium, 
    count( distinct ga_session_id ) as sessions, 
    count(distinct case when identification_status = 'connected' then ga_session_id end ) as visits_connected,
    count(distinct case when user_type = 'subscriber' then ga_session_id end ) as visits_abonne,
    count(distinct case when user_type = 'lead' then ga_session_id end ) as visits_prospect,
    count( distinct ga_session_id ) - count(distinct case when identification_status = 'connected' then ga_session_id end ) as visits_notconnected
from ga_data
group by 1,2,3,4,5
order by event_date desc 