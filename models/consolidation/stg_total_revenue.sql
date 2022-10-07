{{
  config(
    materialized = 'table',
    labels = {'type': 'stripe', 'contains_pie': 'no', 'category':'source'}  
   )
}}

WITH stripe_charges_consolidation AS (
    SELECT
        stripe_customer_id as stripe_customer_id,
        receipt_email as email,
        CASE
            WHEN charge_type LIKE '%Abonnement%' THEN 'Abonnement'
            WHEN charge_type LIKE 'Shop' THEN 'Boutique'
            WHEN charge_type LIKE 'PetitPlus' THEN 'PetitPlus'
            ELSE charge_type
        END AS charge_type,
        pr_id,
        pr_name,
        pr_code_postal,
        pr_region,
        SUM(CASE WHEN charge_date <= '2022-06-30' THEN charges_amount ELSE 0 END ) as total_charge_amount_stripe_before_300622,
        SUM(CASE WHEN charge_date > '2022-06-30' THEN charges_amount ELSE 0 END ) as total_charge_amount_stripe_after_300622, 
        SUM(CASE WHEN charge_date <= '2022-06-30' THEN amount_refunded ELSE 0 END) as total_refund_amount_stripe_before_300622,
        SUM(CASE WHEN charge_date > '2022-06-30' THEN amount_refunded ELSE 0 END) as total_refund_amount_stripe_after_300622,
    FROM
        {{ ref('stg_charges_consolidation')}}
    GROUP BY 
        stripe_customer_id,
        receipt_email,
        charge_type,
        pr_id,
        pr_name,
        pr_code_postal,
        pr_region
),

stripe_coupon AS (
    SELECT DISTINCT
        in_date,
        customer,
        description,
        in_amount
    FROM
        {{ ref('src_stripe_invoice_items')}}
    WHERE LOWER(description) LIKE '%coupon%' 
),

stripe_total_coupon_used AS (
    SELECT
        customer,
        SUM(in_amount) AS stripe_total_amount_coupon_used
    FROM stripe_coupon
    GROUP BY customer
),

mongodb_sale_consolidation_by_date AS (
    SELECT
        customerid as mongodb_customer_id,
        email as email,
        sale_date,
        CASE
            WHEN type_sale LIKE 'Boutique' THEN 'Boutique'
            WHEN type_sale LIKE 'Abonnement' THEN 'Abonnement'
            WHEN type_sale LIKE 'Petit plus' THEN 'PetitPlus'
            ELSE type_sale
        END AS type_sale,
        place_id,
        place_name,
        place_city,
        place_codepostal,
        nom_region,
        nom_departement,
        SUM(price_details_ttc) as total_mongo_sale_by_date,
        --AVG(place_coupon_amount) as coupon_amount_used_mongo_by_date
    FROM
        {{ ref('stg_mongo_sale_consolidation')}}
    GROUP BY
        customerid,
        email,
        sale_date,
        type_sale,
        place_id,
        place_name,
        place_city,
        place_codepostal,
        nom_region,
        nom_departement
),

mongodb_sale_consolidation AS (
    SELECT
        mongodb_customer_id,
        email,
        type_sale,
        place_id,
        place_name,
        place_city,
        place_codepostal,
        nom_region,
        nom_departement,
        SUM(total_mongo_sale_by_date) AS total_mongo_sale,
        --SUM(coupon_amount_used_mongo_by_date) AS total_coupon_used_mongo
    FROM
        mongodb_sale_consolidation_by_date
    GROUP BY
        mongodb_customer_id,
        email,
        type_sale,
        place_id,
        place_name,
        place_city,
        place_codepostal,
        nom_region,
        nom_departement  
)

SELECT
    COALESCE(stripe_charges_consolidation.stripe_customer_id, mongodb_sale_consolidation.mongodb_customer_id) AS customer_id,
    COALESCE(stripe_charges_consolidation.email, mongodb_sale_consolidation.email) AS email,
    COALESCE(stripe_charges_consolidation.pr_id, mongodb_sale_consolidation.place_id) AS place_id,
    COALESCE(stripe_charges_consolidation.pr_name, mongodb_sale_consolidation.place_name) AS pr_name,
    COALESCE(stripe_charges_consolidation.charge_type,mongodb_sale_consolidation.type_sale) AS type_sale,
    COALESCE(stripe_charges_consolidation.pr_region, mongodb_sale_consolidation.nom_region) AS region,
    stripe_charges_consolidation.total_charge_amount_stripe_before_300622,
    stripe_charges_consolidation.total_charge_amount_stripe_after_300622,
    stripe_charges_consolidation.total_refund_amount_stripe_before_300622,
    stripe_charges_consolidation.total_refund_amount_stripe_after_300622,
    ROUND(mongodb_sale_consolidation.total_mongo_sale,2) AS total_mongo_sale,
    stripe_total_coupon_used.stripe_total_amount_coupon_used/100 AS stripe_total_amount_coupon_used

FROM stripe_charges_consolidation 
FULL OUTER JOIN mongodb_sale_consolidation
ON 
  stripe_charges_consolidation.stripe_customer_id = mongodb_sale_consolidation.mongodb_customer_id
  AND stripe_charges_consolidation.pr_id = mongodb_sale_consolidation.place_id
  AND stripe_charges_consolidation.charge_type = mongodb_sale_consolidation.type_sale
FULL JOIN stripe_total_coupon_used
ON
    stripe_charges_consolidation.stripe_customer_id = stripe_total_coupon_used.customer
--WHERE COALESCE(stripe_charges_consolidation.stripe_customer_id, mongodb_sale_consolidation.mongodb_customer_id) = 'cus_H6i4PbYFfK5pQu'
