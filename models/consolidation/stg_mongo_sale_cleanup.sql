{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}


WITH  
  sale_data AS (
    select distinct 
      shippingat,
      cast(shippingat as date) as shipping_date,
      DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
      DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
      LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,        
      sale_id,
      place_id, 
      company, 
      firstname,
      lastname,
      phone,
      user_id,
      email,
      createdat,
      subscription_id,
      shipping_addresse,
      shipping_city,
      shipping_codepostal,
      price_ttc as price_ttc_raw,
      --round(cast(price_ttc as int64)/100,2) as price_ttc,
      -- round(cast(offerings_value_price_ttc as int64)/100,2) as price_ttc,  
      refundedprice /100 as amount_refund,
      customerid,
      subscriptionid, 
      subscription_rate,
      subscription_status,
      case when subscription_rate = 'biweekly' then 'Livraison chaque quinzaine'
          when subscription_rate = 'weekly' then 'Livraison chaque semaine'
          when subscription_rate = 'fourweekly' then 'Livraison chaque mois'
          end as subscription_type,   
      subscription_total_casiers,
      channel,
      offerings_value_channel,
      CASE WHEN channel = 'shop' THEN 'Boutique'
          WHEN  channel = 'combo' and offerings_value_channel = 'combo' THEN 'Abonnement'
          WHEN  channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
      END AS type_sale,  
      round(cast(offerings_value_price_ttc as int64)/100,2) as price_details_ttc,
      offerings_value_price_ttc,
      offerings_value_price_tax,
      offerings_value_price_ht,
      subscription_price,
      offerings_value_count,
      offerings_value_name,
      -- offerings_value_items_value_product_name,
      --offerings_value_items_value_product_id,
      --offerings_value_items_value_product_type,
      --invoiceitemid,
      --chargeid,
      status, 
    FROM  {{ ref('src_mongodb_sale') }} 
    where status is null
    or status = 'paid'
    order by subscription_total_casiers asc 
  ),

  sale_data_ttc_bonus as (
    select distinct

      shippingat,
      shipping_date,
      sale_date,
      first_day_week,
      last_day_week,        
      sale_id,
      place_id, 
      company, 
      firstname,
      lastname,
      phone,
      user_id,
      email,
      createdat,
      subscription_id,
      shipping_addresse,
      shipping_city,
      shipping_codepostal,
      price_ttc_raw,
      amount_refund,
      customerid,
      subscriptionid, 
      subscription_rate,
      subscription_status,
      subscription_type,   
      subscription_total_casiers,
      channel,
      offerings_value_channel,
      type_sale,  
      subscription_price,
      --chargeid,
      -- status, 
      
      case
        when type_sale = 'Boutique' then round(cast(offerings_value_price_ttc*offerings_value_count as float64)/100,2)
        when type_sale = 'Abonnement' then round(cast(price_ttc_raw as float64)/100,2) 
        when type_sale = 'Petit plus' then round(cast(price_ttc_raw - subscription_price as float64)/100,2)  
      end as price_ttc,

      case
        when type_sale = 'Boutique' then round(cast(offerings_value_price_ttc*offerings_value_count as float64)/100,2)
      end as sale_boutique_ttc,
      case 
        when type_sale = 'Abonnement' and 'Petit plus' in (select type_sale from sale_data s1 where s1.sale_id = sale_data.sale_id) then round(cast(subscription_price as float64)/100,2)
        when type_sale = 'Abonnement' then round(cast(price_ttc_raw as float64)/100,2) 
      end as sale_locker_ttc,
      case
        when type_sale = 'Petit plus' then round(cast(price_ttc_raw - subscription_price as float64)/100,2)  
      end as sale_bonus_ttc,

    from sale_data
  ),

  data_cp as (
    SELECT
      in_date,
      in_id,
      in_invoice,
      in_subscription_id,
      round(in_amount/100,2) as coupon_value,
      customer,
      description,
      SPLIT(description, ' ')[OFFSET(1)] as coupon_name
    FROM
      {{ ref('src_stripe_invoice_items') }}
    WHERE
      description LIKE 'Coupon%'
  ),

  data_cp_with_src as (
    select
      data_cp.*,
      Type as coupon_source
    from data_cp
    left join {{ ref('src_external_coupon') }} src_cp
    on data_cp.coupon_name = src_cp.coupon
  ),

  data_score_conso as (
    select
      *
    from {{ ref('stg_external_score_conso_data') }}
  )

select 
  sale_data_ttc_bonus.*,
  IFNULL(sale_bonus_ttc,0)+IFNULL(sale_boutique_ttc,0)+IFNULL(sale_locker_ttc,0) as sale_total_ttc,
  coupon_value,
  coupon_name,
  coupon_source,
  avg_product_sccore,
  avg_display_score,
  avg_method_score,
  avg_command_score
from sale_data_ttc_bonus
left join data_cp_with_src
on 
  sale_data_ttc_bonus.sale_date = data_cp_with_src.in_date
  AND sale_data_ttc_bonus.customerid = data_cp_with_src.customer
left join data_score_conso
on
  sale_data_ttc_bonus.sale_id = data_score_conso.transaction_id
  and sale_data_ttc_bonus.type_sale = data_score_conso.type_sale