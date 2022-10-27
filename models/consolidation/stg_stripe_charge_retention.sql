{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}
WITH charges AS (
    SELECT
    distinct 
        id as charge_id,
        receipt_email,
        balance_transaction,
        created,
        payment_intent,
        receipt_number,
        paid,
        invoice AS charges_invoice,
        currency,
        payment_method,
        --receipt_url,
        failure_code,
        failure_message,
        status,
        refunded,
        captured,
        source_exp_year,
        source_object,
        source_country,
        source_id,
        source_name,
        source_customer,
        source_address_country,
        amount AS charges_amount,
        amount_refunded,
        disputed,
        updated,
        customer,
        dispute,
        shipping_address_country,
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY _sdc_extracted_at DESC) AS rn 
    FROM {{ ref('src_stripe_charges')}}  
    WHERE  status = 'succeeded' 
),

final_charges AS (
    SELECT 
        distinct
        -- created as charge_time_stamp,
        cast(created as date) as charge_date,
        extract(year from created) as charge_year,
        extract(month from created) as charge_month,
        cast(LAG(created) OVER (PARTITION BY customer ORDER BY created DESC) as date) as prev_charge_date,
        charges_amount,
        amount_refunded,
        charges.charge_id, 
        receipt_email,
        balance_transaction,
        payment_intent,
        receipt_number,
        customer,
        paid,
        charges_invoice,
        currency,
        payment_method,
        -- receipt_url,
        failure_code,
        failure_message,
        status,
        refunded,
        captured,       
    FROM charges 
    WHERE rn=1
),

result as (
    select
        distinct
        -- charge_time_stamp,
        charge_id, 
        customer,
        charge_date,
        charge_year,
        charge_month,
        prev_charge_date,
        date_diff(charge_date,prev_charge_date,month) as months_from_prev_charge,
        date_diff(charge_date,prev_charge_date,day) as days_from_prev_charge,
        
        charges_amount,
        amount_refunded,
        receipt_email,
        balance_transaction,
        payment_intent,
        receipt_number,
        paid,
        charges_invoice,
        currency,
        payment_method,
        failure_code,
        failure_message,
        status,
        refunded,
        captured,       


    from final_charges
)

select * from result