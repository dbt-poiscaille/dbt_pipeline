{{
  config(
    materialized = 'table',
    labels = {'type': 'mongo', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

with stock_data as (
    select
        _id as stock_id,
        item.supplier.name as supplier_name,
        item.supplier.boat.name as supplier_boat_name,
        -- buyer._id as buyer_id,
        -- buyer.email as buyer_email,
        -- cast(createdat as date) as stock_createdat,

        -- item.product._id as item_product_id,
        -- item.product.name as stock_item_product_name,

        item.allocation.slicing.portion.unit as stock_item_allocation_slicing_portion_unit,
        item.allocation.slicing.portion.quantity as stock_item_allocation_slicing_portion_quantity,
        item.allocation.slicing.whole.quantity as item_allocation_slicing_whole_quantity,
        item.allocation.slicing.inventory.initial as item_allocation_slicing_inventory_initial,
        -- adjustments.value.quantity as adjustments_value_quantity,
        -- adjustments.value.type as adjustments_value_type,
        -- item.description as item_description,
        
        -- cost.ttc as stock_cost_ttc,
        -- cost.ht as stock_cost_ht,

        -- -- calibersorter,
        -- item.meta.caliber as item_meta_caliber,

        cast(item.allocation.available.bought as date) as item_allocation_available_bought, 
        cast(item.allocation.available.arrival as date) as item_allocation_available_arrival,
        cast(item.allocation.available.from as date) as item_allocation_available_from, 
        cast(item.allocation.available.to as date) as item_allocation_available_to,

    from {{ ref('src_mongodb_stock') }}
    -- unnest(adjustments) as adjustments
),

sale_offering_data as (
    select 
        sale_date,
        sale_id, 
        sale_price_ttc,
        subscription_price,
        offerings_value_channel,
        channel,
        type_sale,
        offerings_value_id, 
        offerings_value_name,
        sale_product_id,
        sale_product_type,
        sale_product_name,
        offerings_value_count, 
        offerings_value_price_ttc,

        offering_id,
        
        offering_price_ttc, 
        offering_price_ht,

        items_value_allocations_stock_id,
        offerings_value_items_value_portion_quantity,
        offerings_value_items_value_portion_unit,
        items_value_product_name,
        item_value_product_id,

        offerings_value_items_value_cost_ttc,


    from {{ ref('stg_mongo_sale_offering_consolidation') }}
),

result as (
    select distinct
        *

    from sale_offering_data
    left join stock_data on stock_data.stock_id = sale_offering_data.items_value_allocations_stock_id
)

select * from result
-- where sale_id = '63065f18a60db765490dd62e'
-- where stock_id = '62bd900b2033c49b54a96bd7'
-- and offerings_value_id = '62e51697dd7ad27a156e7993'

