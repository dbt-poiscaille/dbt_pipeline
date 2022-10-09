{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

with consolidation_ga as (

select 
    event_date,
    event_name,
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
) , 

 consolidation_int as (

select 
       event_date, 
       device_category, 
       user_id,
       user_pseudo_id,
       ga_session_id,
       traffic_name,
       traffic_source,
       traffic_medium,
       max(medium) as medium, 
       max(campaign) as campaign, 
       max(source) as source, 
       max(user_type) as user_type, 
       max(session_engaged) as session_engaged,
       count( distinct case when event_name = 'screenInteraction' then ga_session_id  end ) as interactions, 
       count( distinct case when event_name = 'signInCheck' then ga_session_id  end ) as signInCheck, 
       count( distinct case when event_name = 'orderComplete' then ga_session_id  end) as orderComplete, 
       count( distinct case when event_name = 'checkout' then ga_session_id  end) as checkout, 
       count( distinct case when event_name = 'add_to_cart' then ga_session_id  end) as add_to_cart, 
       count( distinct case when event_name = 'purchase' then ga_session_id  end ) as purchase, 
       count( distinct case when event_name = 'first_visit' then ga_session_id  end ) as first_visit, 
       count( distinct case when event_name = 'session_start' then ga_session_id  end ) as session_start, 
       count( distinct case when event_name = 'signUp' then ga_session_id  end) as signUp,
       count( distinct case when event_name = 'signIn' then ga_session_id  end) as signIn
         from consolidation_ga
  --where ga_session_id = 1664446638
  group by 1,2,3,4,5,6,7,8

 )

 select 
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      device_category, 
      medium,
      campaign, 
      source,
      traffic_name,
      traffic_source,
      traffic_medium,
      count(distinct ga_session_id) as sessions, 
      sum(session_start) as sessions_start ,       
      count(distinct user_pseudo_id) as users_ga, 
      count(distinct user_id) as user_id_poiscaille, 
      sum(cast(session_engaged as int64)) as session_engaged, 
      sum(interactions) as session_interactions, 
      sum(signUp) as session_signUp ,
      sum(signIn) as session_signIn ,
      sum(signInCheck) as session_signincheck ,
      sum(orderComplete) as session_order_complete, 
      sum(checkout) as session_checkout, 
      sum(add_to_cart) as session_addtocart,
      sum(purchase) as session_purchase , 
      sum(first_visit) as session_firt_visit
    from consolidation_int
      group by 1,2,3,4,5,6,7,8
   order by event_date desc 








































