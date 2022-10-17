{{
  config(
    materialized = 'table',
    labels = {'type': 'mongo', 'contains_pie': 'yes', 'category':'production'}  
   )
}}

with sale_data as (
    select
        distinct
        DATE_ADD(cast(shippingat as date), INTERVAL 1 DAY) as sale_date,
        sale_id, 
        price_ttc as sale_price_ttc,
        subscription_price,
        offerings_value_channel,
        channel,
        CASE WHEN channel = 'shop' THEN 'Boutique'
            WHEN  channel = 'combo' and offerings_value_channel = 'combo' THEN 'Abonnement'
            WHEN  channel = 'combo' and offerings_value_channel = 'shop' THEN 'Petit plus'
        END AS type_sale,
        offerings_value_id, 
        offerings_value_name,
        offerings_value_items.value.product._id as sale_product_id,
        offerings_value_items.value.product.name as sale_product_name,
        offerings_value_count, 
        offerings_value_price_ttc,

    from {{ ref('src_mongodb_sale')}},
    unnest(offerings_value_items) as offerings_value_items

),

offering_data as (
    SELECT
    _id as offering_id,
    
    price.ttc as offering_price_ttc, 
    price.ht as offering_price_ht,

    items_value_allocations.value.stock._id as items_value_allocations_stock_id,
    items_value_allocations.value.slicing.portion.quantity as items_value_allocations_slicing_portion_quantity,
    items_value_allocations.value.slicing.portion.unit as items_value_allocations_slicing_portion_unit,
    items.value.product.name as items_value_product_name,
    items.value.product.type as items_value_product_type,
    items.value.product._id as item_value_product_id,

    items_value_allocations.value.slicing.portion.unit as items_value_allocations_value_slicing_portion_unit,
    items_value_allocations.value.slicing.portion.quantity as items_value_allocations_value_slicing_portion_quantity, 
    -- items_value_allocations.value.slicing.whole.unit as items_value_allocations_value_slicing_whole_unit, 
    -- -- items_value_allocations.value.slicing.whole.quantity as items_value_allocations_value_slicing_whole_quantity, 
    -- -- items_value_allocations.value.slicing.inventory.initial as items_value_allocations_value_slicing_inventory_initial,

    items_value_allocations.value.cost.ttc as items_value_allocations_value_cost_ttc,
    items_value_allocations.value.cost.currency as items_value_allocations_value_cost_currency,
    items_value_allocations.value.cost.unit as items_value_allocations_value_cost_unit,

    -- cast(items_value_allocations.value.available.bought as date) as items_value_allocations_value_available_bought, 
    -- cast(items_value_allocations.value.available.from as date) as items_value_allocations_value_avalaible_from, 
    -- cast(items_value_allocations.value.available.to as date) as items_value_allocations_value_available_to,



    FROM {{ ref('src_mongodb_offering') }},
    unnest(items) as items,
    unnest(items.value.allocations) as items_value_allocations
)

select distinct * 
from sale_data left join offering_data
on sale_data.offerings_value_id = offering_data.offering_id
and sale_data.sale_product_id = offering_data.item_value_product_id
-- where type_sale = 'Abonnement'
-- where sale_id = '6345b066181d165007761ee1'
-- and offerings_value_id = '62e51697dd7ad27a156e7993'

