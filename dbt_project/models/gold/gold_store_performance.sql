{{
  config(
    materialized='table',
    tags=['gold', 'store_analytics']
  )
}}

-- Gold layer: Store performance metrics for business analytics
-- Aggregates inventory value, product counts, and operational metrics by store

WITH stores AS (
    SELECT * FROM {{ ref('silver_stores') }}
),

inventory AS (
    SELECT
        store_id,
        SUM(inventory_value) AS total_inventory_value,
        COUNT(DISTINCT product_id) AS unique_products_in_stock,
        SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) AS products_needing_reorder,
        AVG(inventory_value) AS avg_inventory_value_per_product,
        SUM(CASE WHEN stock_status = 'Critical' THEN 1 ELSE 0 END) AS critical_stock_items,
        SUM(CASE WHEN stock_status = 'Overstock' THEN 1 ELSE 0 END) AS overstock_items,
        MAX(last_restocked_at) AS last_restock_date,
        AVG(days_since_restock) AS avg_days_since_restock
    FROM {{ ref('silver_inventory') }}
    GROUP BY store_id
),

final AS (
    SELECT
        s.store_id,
        s.store_name,
        s.store_type,
        s.city,
        s.state,
        s.region,
        s.store_age_days,
        s.store_age_category,
        s.full_address,
        s.phone,
        s.opened_at,

        -- Inventory metrics
        COALESCE(i.total_inventory_value, 0) AS total_inventory_value,
        COALESCE(i.unique_products_in_stock, 0) AS unique_products_in_stock,
        COALESCE(i.products_needing_reorder, 0) AS products_needing_reorder,
        COALESCE(i.avg_inventory_value_per_product, 0) AS avg_inventory_value_per_product,
        COALESCE(i.critical_stock_items, 0) AS critical_stock_items,
        COALESCE(i.overstock_items, 0) AS overstock_items,

        -- Operational metrics
        i.last_restock_date,
        ROUND(COALESCE(i.avg_days_since_restock, 0), 1) AS avg_days_since_restock,

        -- Store utilization score (simple metric: inventory value per day of operation)
        CASE
            WHEN s.store_age_days > 0 THEN ROUND(COALESCE(i.total_inventory_value, 0) / s.store_age_days, 2)
            ELSE 0
        END AS inventory_value_per_day_open,

        -- Health indicator
        CASE
            WHEN i.critical_stock_items > 5 THEN 'Poor'
            WHEN i.products_needing_reorder > 10 THEN 'Fair'
            WHEN i.overstock_items > 5 THEN 'Fair'
            ELSE 'Good'
        END AS inventory_health_status,

        CURRENT_TIMESTAMP AS calculated_at

    FROM stores s
    LEFT JOIN inventory i ON s.store_id = i.store_id
)

SELECT * FROM final
