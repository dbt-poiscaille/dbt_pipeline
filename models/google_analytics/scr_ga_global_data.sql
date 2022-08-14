{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

with date_range as (
select
    '20220101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date)

select *
from {{ source('google_analytics', 'events_*') }},  
date_range
where
  _table_suffix between date_range.start_date and date_range.end_date

union all 

select *
from  {{ source('google_analytics', 'events_intraday_*') }},   
date_range
where
  _table_suffix between date_range.start_date and date_range.end_date

order by event_date desc 