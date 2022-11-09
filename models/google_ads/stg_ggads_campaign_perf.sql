{{
  config(
    materialized = 'table',
    labels = {'type': 'google_ads', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

select distinct
  campaign_id,
  campaign_name,
  ad_network_type,
  
  -- date
  year,
  campaign_start_date,
  campaign_end_date,
  date,
  campaign_budget_amount_micros,
  campaign_budget_period,

  sum(clicks) as nb_clicks,
  sum(impressions) as impressions,
  sum(all_conversions) as all_conversions,
  sum(all_conversions_value) as all_conversions_value,
  round(sum(cost_micros/1000000),2) as cost_micros,
  sum(conversions_value) as conversions_value,
  sum(invalid_clicks) as invalid_clicks,
  count(distinct customer_id) as nb_customers,
  sum(interactions) as interactions,
  sum(view_through_conversions) as view_through_conversions,
  sum(engagements) as engagements,
  sum(video_views) as video_views,
  avg(interaction_rate) as avg_interaction_rate,
  avg(average_cost) as average_cost,
  avg(average_cpc) as average_cpc,
  avg(average_cpe) as average_cpe,
  avg(average_cpm) as average_cpm,
  avg(average_cpv) as average_cpv


from {{ ref('src_ggads_campaign_performance_report') }}
group by 1,2,3,4,5,6,7,8,9
order by date desc