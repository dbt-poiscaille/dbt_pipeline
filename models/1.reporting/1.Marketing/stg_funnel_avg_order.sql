
{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with sale_data as (
select 
  distinct 
  sale_id,
  shippingat,
  DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
  DATE_TRUNC(cast(shippingat as date), WEEK(MONDAY)) as first_day_week,
  LAST_DAY(cast(shippingat as date), WEEK(MONDAY)) as last_day_week,       
  subscription_status,
  --offerings_value_price_tax,
  --offerings_value_price_ht,
  --margin,
  --margin__fl, 
  subscription_price,
  subscription_id,
  case when subscription_id is null then 'order' else 'locker' end as sale_type,
  CASE WHEN channel = 'shop' THEN 'Boutique'
      WHEN channel = 'combo' and offerings_value_channel = 'combo' THEN 'Casier'
      WHEN channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
  END AS type_sale,    
  offerings_value_price_ttc as revenue,
  price_ttc, 
  price_ht, 
  offerings_value_channel,
  --offerings_value_count,
  --offerings_value_name,
  --offerings_value_items_value_portion_unit,
  --offerings_value_items_value_portion_quantity,
  --offerings_value_item_value_cost_ht,
  --offerings_value_items_value_cost_ttc,
  --offerings_value_items_value_cost_unit,
  --offerings_value_items_value_product_name,
  email,
  user_id,
  name, 
from
 {{ ref('src_mongodb_sale') }}
 order by sale_id asc 
)

select 
    sale_date as sale_data_order , 
    count(distinct sale_id) as transactions, 
    round(sum(revenue)/100,2) as revenue_total, 
    count(distinct case when type_sale='Boutique' then sale_id end  ) as transactions_boutique,
    count(distinct case when type_sale='Casier' then sale_id end  ) as transactions_casier,
    count(distinct case when type_sale='Petit plus' then sale_id end  ) as transactions_pplus, 
    round(sum(case when type_sale='Boutique' then revenue end  )/100,2) as revenue_boutique,
    round(sum(case when type_sale='Casier' then revenue end )/100,2) as revenue_casier,
    round(sum(case when type_sale='Petit plus' then revenue end  )/100,2) as revenue_pplus, 
    sum(revenue) / count(distinct sale_id) as global_pan_my, 
    sum(case when type_sale='Boutique' then revenue end ) / count(distinct case when type_sale='Boutique' then sale_id end  ) as pan_moy_boutique,
    sum(case when type_sale='Casier' then revenue end  ) / count(distinct case when type_sale='Casier' then sale_id end  ) as pan_moy_casier,
    sum(case when type_sale='Petit plus' then revenue end  ) / count(distinct case when type_sale='Petit plus' then sale_id end  ) as pan_moy_pplus        

    from sale_data
    group by 1    
    order by 1 desc   
     