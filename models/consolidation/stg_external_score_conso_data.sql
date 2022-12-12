{{
  config(
    materialized = 'table',
    labels = {'type': 'external', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

with 
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
    ),

    sale_data_detail as (
        SELECT DISTINCT 
            sale_id as transaction_id ,
            type_sale,  
            offerings_value_name as panier , 
            offerings_value_items_value_product_name as product ,
            offerings_value_items_value_product_id as product_id,
            offerings_value_items_value_product_type as product_type,
            offerings_value_items_value_portion_unit as portion_unit,
            offerings_value_items_value_portion_quantity as portion_quantity,
            offerings_value_items_value_piececount as piece,
            offerings_value_items_value_meta_method as method,
            offerings_value_items[SAFE_OFFSET(0)].value.meta.display.name as display,
            offerings_value_items_value_meta_caliber as caliber,
            cast(SPLIT(replace(offerings_value_items_value_meta_caliber,'+',''),'/')[SAFE_OFFSET(0)] as string) as min_caliber,
            cast(SPLIT(replace(offerings_value_items_value_meta_caliber,'+',''),'/')[SAFE_OFFSET(1)] as string) as max_caliber,
            
        from {{ ref('src_mongodb_sale') }}
    ),

    sale_date_portion_quantity as (
        select
            *,
            case
                when portion_unit = 'piece' then ifnull((SAFE_CAST(max_caliber as int64) + SAFE_CAST(min_caliber as int64))/2,SAFE_CAST(min_caliber as int64))*portion_quantity
                when portion_unit = 'pieceAsDozen' then 1000*portion_quantity/12
                else portion_quantity
            end as portion_quantity_caliber
        from sale_data_detail
        order by transaction_id
    ),

    sale_date_score_conso_init as (
        select distinct
            sale.*,
            product_type_conso,
            product_score,
            display_type_conso,
            display_score,
            method_type_conso,
            method_score
        from sale_date_portion_quantity as sale
        left join score_data_product 
        on trim(lower(score_data_product.product), ' ') = concat(trim(lower(sale.product), ' '))
        left join score_data_method 
        on trim(lower(score_data_method.method),' ') = concat(trim(lower(sale.method),' '))
        left join score_data_display 
        on trim(lower(score_data_display.display),' ') = concat(trim(lower(sale.display),' '))
    ),

    sale_data_score_detail as (
        select 
            transaction_id,
            type_sale,
            round(safe_divide(sum(ifnull(portion_quantity_caliber,0)*product_score), sum(ifnull(portion_quantity_caliber,0))), 1) as avg_product_score,
            round(safe_divide(sum(ifnull(portion_quantity_caliber,0)*display_score), sum(ifnull(portion_quantity_caliber,0))), 1) as avg_display_score,
            round(safe_divide(sum(ifnull(portion_quantity_caliber,0)*method_score), sum(ifnull(portion_quantity_caliber,0))), 1) as avg_method_score,
        from sale_date_score_conso_init
        group by 1,2
    ),

    result as (
        select
            *,
            round(avg_product_score + avg_display_score + avg_method_score,1) as avg_command_score
        from sale_data_score_detail
        order by transaction_id
    )

select
    *
from result

-- order by sale_date desc, sale_id asc