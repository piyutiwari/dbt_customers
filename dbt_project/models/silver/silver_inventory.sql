{{
  config(
    materialized='table',
    tags=['silver', 'inventory']
  )
}}

-- Silver layer: Enriched inventory data with product and store details
-- Calculates inventory value, stock status, and reorder flags

WITH inventory AS (
    SELECT * FROM {{ ref('bronze_inventory') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category,
        cost,
        price
    FROM {{ ref('silver_products') }}
),

stores AS (
    SELECT
        store_id,
        store_name,
        city,
        state
    FROM {{ ref('silver_stores') }}
),

enriched_inventory AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.store_id,
        i.quantity_on_hand,
        i.reorder_point,
        i.reorder_quantity,
        i.last_restocked_at,

        -- Enrichment from products
        p.product_name,
        p.category AS product_category,
        p.cost AS product_cost,
        p.price AS product_price,

        -- Enrichment from stores
        s.store_name,
        s.city AS store_city,
        s.state AS store_state,

        -- Calculate inventory value using product cost
        i.quantity_on_hand * COALESCE(p.cost, 0) AS inventory_value,

        -- Calculate days since last restock
        DATEDIFF(day, i.last_restocked_at, CURRENT_DATE) AS days_since_restock,

        -- Flag if needs reorder
        CASE
            WHEN i.quantity_on_hand <= i.reorder_point THEN TRUE
            ELSE FALSE
        END AS needs_reorder,

        -- Calculate stock level percentage
        CASE
            WHEN i.reorder_quantity > 0 THEN ROUND((i.quantity_on_hand::FLOAT / i.reorder_quantity * 100), 2)
            ELSE 0
        END AS stock_level_percentage,

        -- Classify inventory status
        CASE
            WHEN i.quantity_on_hand <= 0 THEN 'Critical'
            WHEN i.quantity_on_hand <= i.reorder_point THEN 'Low'
            WHEN i.quantity_on_hand > (i.reorder_quantity * 2) THEN 'Overstock'
            ELSE 'Normal'
        END AS stock_status,

        CURRENT_TIMESTAMP AS processed_at

    FROM inventory i
    LEFT JOIN products p ON i.product_id = p.product_id
    LEFT JOIN stores s ON i.store_id = s.store_id
)

SELECT * FROM enriched_inventory
