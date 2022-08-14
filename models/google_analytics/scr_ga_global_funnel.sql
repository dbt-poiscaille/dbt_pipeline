{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'staging'}  
   )
}}

select
      event_date, 
      device.category,
      traffic_source.medium, 
      traffic_source.source, 
      traffic_source.name, 
      count(distinct case when event_name ='first_visit' then user_pseudo_id end) as first_visit,
      count(distinct case when event_name ='session_start' then user_pseudo_id end) as session_start,
      count(distinct case when event_name ='screenInteraction' then user_pseudo_id end) as screenInteraction,
      count(distinct case when event_name ='view_search_results' then user_pseudo_id end) as view_search_results,
      count(distinct case when event_name ='add_to_cart' then user_pseudo_id end) as add_to_cart,
      count(distinct case when event_name ='remove_from_cart' then user_pseudo_id end) as remove_from_cart,
      count(distinct case when event_name ='purchase' then user_pseudo_id end) as purchase,
      count(distinct case when event_name ='screenInteraction' and  (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' then user_pseudo_id end) as subscriptionFunnel,
      count(distinct case when event_name ='screenInteraction' and  (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' then user_pseudo_id end) as outsideFunnel,
      count(distinct case when event_name ='screenInteraction' and  (select value.string_value from unnest(event_params) where key = 'checkout_category')='shopFunnel' then user_pseudo_id end) as shopFunnel,
      count(distinct case when event_name ='screenInteraction' and  (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' then user_pseudo_id end) as outsideFunnel,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='searchPlace' then user_pseudo_id end) as subscription_funnel_searchPlace,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='selectPlace' then user_pseudo_id end) as subscription_funnel_selectPlace,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='viewPlace' then user_pseudo_id end) as subscription_funnel_viewPlace,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='allergies' then user_pseudo_id end) as subscription_funnel_allergies,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='quantity' then user_pseudo_id end) as subscription_funnel_quantity,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='rate' then user_pseudo_id end) as subscription_funnel_rate,
      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='subscriptionFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='login' then user_pseudo_id end) as subscription_funnel_login,         

      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='searchPlace' then user_pseudo_id end) as outside_funnel_searchPlace,

      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='viewPlace' then user_pseudo_id end) as outside_funnel_searchPlace,

      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='selectPlace' then user_pseudo_id end) as outside_funnel_searchPlace,

      count(distinct case when event_name ='screenInteraction' 
           and (select value.string_value from unnest(event_params) where key = 'checkout_category')='outsideFunnel' 
           and  (select value.string_value from unnest(event_params) where key = 'event_action')='login' then user_pseudo_id end) as outside_funnel_searchPlace

    FROM {{ ref('scr_ga_global_data') }}
    group by 1,2,3,4,5