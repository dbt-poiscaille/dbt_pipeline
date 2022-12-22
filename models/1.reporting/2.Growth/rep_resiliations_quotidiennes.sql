{{
    config(
        materialized='table',
        labels={'type': 'mongodb', 'contains_pie': 'yes', 'category': 'production'},
    )
}}

select
    cast(unsubscribed.at as date) as unsubscribed_date,
    unsubscribed.reason,
    unsubscribed.detailedreason,
    concat('https://poiscaille.fr/kraken/client/', user._id) as link,
    user.email as email
from {{ source('mongodb', 'subscription') }}
where unsubscribed.at is not null
