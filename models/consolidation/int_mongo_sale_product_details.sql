{{
  config(
    materialized = 'table',
    labels = {'type': 'mongodb', 'contains_pie': 'no', 'category':'source'}  
   )
}}

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
    offerings_value_items_value_meta_caliber as caliber,
    offerings_value_items_value_description as description,
    offerings_value_items_value_image_url as image_url,

FROM {{ ref('stg_detail_products_sales') }}
ORDER BY sale_id