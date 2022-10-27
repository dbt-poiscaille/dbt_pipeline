{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with charge_data as (
    select 
        charge_id, 
        customer,
        charge_date,
        charge_year,
        charge_month,
        prev_charge_date,
        months_from_prev_charge,
        days_from_prev_charge,
        charges_amount,
        amount_refunded,
        receipt_email,
        -- balance_transaction,
        -- payment_intent,
        -- receipt_number,
        -- paid,
        -- charges_invoice,
        -- currency,
        -- payment_method,
        -- failure_code,
        -- failure_message,
        -- status,
        -- refunded,
        -- captured,       
    from {{ ref('stg_stripe_charge_retention') }}
),

mongo_user as (
    select
    _id as user_id_mongodb,
    customer as user_id_stripe,
    firstname,
    lastname,
    case when customer is null then 'No StripeId' else 'StripeId'end id_stripe_status,
    --case when customer is null then 'Prospect' else 'Customers'end users_type,
    formula as type_abo,
    (concat(UPPER(lastname),' ',INITCAP(firstname))) as name, 
    role,
    godfather,
    email,
    phone,
    createdat,
    comments,
    newsletter,
    last4,
    iat,
    --godsons,
    formula
        
    from
        {{ ref('src_mongodb_users') }}
    order by user_id_mongodb asc 

),

result as (
    select distinct 
        charge_data.*,
        user_id_mongodb,
        user_id_stripe,
        firstname,
        lastname,

    from charge_data
    left join mongo_user on charge_data.receipt_email = mongo_user.email
)

select * from result
order by charge_date desc





