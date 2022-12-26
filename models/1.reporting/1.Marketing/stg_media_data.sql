{{
    config(
        materialized='table',
        labels={'type': 'funnel', 'contains_pie': 'no', 'category': 'source'},
    )
}}

select
    date,
    'facebookads' as media_type,
    clicks as clicks,
    impressions as impr,
    cost as cost,
    campaign_name__facebook_ads as campaign,
    website_subscriptions__facebook_ads as subscriptions,
    purchases__facebook_ads as purchases,
    website_purchases__facebook_ads as website_purchases,
    website_subscriptions__facebook_ads as website_subscriptions,
    link_clicks__facebook_ads as clicks_links,
    amount_spent__facebook_ads as spend,
    clicks_all__facebook_ads as clicks_all,
    impressions__facebook_ads as impressions,
    website_purchases_conversion_value__facebook_ads as website_purchase_value,
    subscriptionfunnel__step10__facebook_ads as website_subscription_value ,
    purchases_conversion_value__facebook_ads as purchase_value,
    subscribe_conversion_value__facebook_ads as subscription_value
from {{ source('funnel', 'media_data') }}
where data_source_type = 'facebookads'

union all

select
    date,
    'adwords' as media_type,
    clicks as clicks,
    impressions as impr,
    cost as cost,
    campaign__adwords as campaign,
    abonnement_poiscaille___googleads__adwords as subscriptions,
    achat_poiscaille___googleads__adwords as purchases,
    null as website_purchases, 
    abonnement_poiscaille___googleads__adwords as website_subscriptions, 
    null as clicks_links, 
    cost__adwords as spend, 
    clicks__adwords as clicks_all,   
    Impressions__AdWords as impressions,
    all_conv__value__achat_poiscaille___googleads__adwords as website_purchase_value, 
    conv__value__abonnement_poiscaille___googleads__adwords as website_subscription_value,
    conv__value__achat_poiscaille___googleads__adwords as purchase_value,
    conv__value__abonnement_poiscaille___googleads__adwords as subscription_value 
from {{ source('funnel', 'media_data') }}
where data_source_type = 'adwords'
