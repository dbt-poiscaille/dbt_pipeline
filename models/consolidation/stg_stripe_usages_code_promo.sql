{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with data_invoice as ( 
SELECT
 distinct 
  in_date,
  customer,
  description,
  in_invoice,
  in_id
FROM
  {{ ref('src_stripe_invoice_items') }}
  where description not like '%tit plus%'
), 
data_charges as (
SELECT
  created,
  customer,
  receipt_email,
  amount_refunded,
  amount_captured, 
  receipt_number,
  invoice,
  paid,
  id,
  status,
  captured,
  refunded,
  amount,
  description
FROM
    {{ ref('src_stripe_charges') }}
where status = 'succeeded'
)

select 
  distinct 
  data_charges.created as charges_created,
  data_charges.customer as charges_customer,
  data_charges.receipt_email as charges_receipt_email,
  data_charges.amount_captured as charges_amount_captured ,
  data_charges.amount_refunded as charges_amount_refunded,
  data_charges.invoice as charges_invoice,
  data_charges.paid as charges_paid,
  data_charges.id as charges_id,
  data_charges.status as charges_status,
  data_charges.captured as charges_captured,
  data_charges.refunded as charges_refunded,
  data_charges.amount as charges_amount,
  data_charges.description as charges_description, 
  data_invoice.in_invoice,
  data_invoice.description,
  data_invoice.in_id
  from data_charges
  left join data_invoice 
  on data_charges.invoice = data_invoice.in_invoice 
  order by data_invoice.description desc 

  