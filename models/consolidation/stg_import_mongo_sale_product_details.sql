{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

with 
  sale_product_detail as (
    SELECT DISTINCT
        sale_id as transaction_id ,
        CASE WHEN channel = 'shop' THEN 'Boutique'
            WHEN  channel = 'combo' and offerings_value_channel = 'combo' THEN 'Abonnement'
            WHEN  channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
        END AS type_sale,  
        offerings_value_name as panier , 
        offerings_value_items_value_product_name as product ,
        offerings_value_items_value_product_id as product_id,
        offerings_value_items_value_product_type as product_type,
        offerings_value_items_value_portion_unit as portion_unit,
        offerings_value_items_value_portion_quantity as portion_quantity,
        offerings_value_items_value_piececount as piece,
        offerings_value_items_value_cost_ttc as cost_ttc,
        offerings_value_items_value_cost_unit as cost_unit,
        offerings_value_items_value_meta_method as method,
        offerings_value_items_value_meta_display_name as display,
        offerings_value_items_value_meta_display_packaging as packaging,
        offerings_value_items_value_meta_caliber as caliber,
        offerings_value_items_value_description as description,
        offerings_value_items_value_image_url as image_url,
        offerings_value_items_value_supplier_harbor_name as harbor_name,
        round(cast(offerings_value_price_ttc as int64)/100,2) as unit_price

    FROM {{ ref('stg_detail_products_sales') }}
  ),

  score_data_product as (
      select
          product as product,
          type as product_type_conso,
          score as product_score
      from {{ ref('src_external_score_conso') }}
      where type like '%Nageoire%'
  ),

  score_data_method as (
      select
          product as method,
          type as method_type_conso,
          score as method_score
      from {{ ref('src_external_score_conso') }}
      where type like '%Hameçon%'
  ),

  score_data_display as (
      select
          product as display,
          type as display_type_conso,
          score as display_score
      from {{ ref('src_external_score_conso') }}
      where type like '%Lame%'
  )


SELECT
  sale_product_detail.*,
  lower(concat(product_id,'_',sale_product_detail.product,'_',ifnull(caliber,''),'_',ifnull(sale_product_detail.method,''),'_',ifnull(sale_product_detail.display,''),'_',ifnull(packaging,''),'_',ifnull(harbor_name,''))) as unique_id,

  product_type_conso,
  display_type_conso,
  method_type_conso,

FROM sale_product_detail
left join score_data_product 
on trim(lower(score_data_product.product), ' ') like concat('%', trim(lower(sale_product_detail.product), ' '), '%')
left join score_data_method 
on trim(lower(score_data_method.method),' ') like concat('%', trim(lower(sale_product_detail.method),' '), '%')
left join score_data_display 
on trim(lower(score_data_display.display),' ') like concat('%', trim(lower(sale_product_detail.display),' '), '%')
ORDER BY transaction_id