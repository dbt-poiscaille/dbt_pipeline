{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}
{%- set my_query -%}
SELECT distinct event_name  from  {{ ref('scr_ga_global_data') }} ;
{%- endset -%}

{%- set results = run_query(my_query) -%}
{%- if execute -%} {%- set events = results.columns[0].values() -%} {%- endif %}

select distinct
    event_date,
    device.category as device_category,
    traffic_source.name as traffic_name,
    traffic_source.medium as traffic_medium,
    traffic_source.source as traffic_source,
    count(distinct user_pseudo_id) as nb_utilisateurs,
    {% for i in events %} count(distinct case when event_name = '{{i}}' then user_pseudo_id end) as total_{{ i }}, {% endfor %}

from {{ ref('scr_ga_global_data') }}
group by 1,2,3,4,5
order by 1 desc