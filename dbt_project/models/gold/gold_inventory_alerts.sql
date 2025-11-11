{{
  config(
    materialized='table',
    tags=['gold', 'inventory_alerts', 'operational']
  )
}}

-- Gold layer: Actionable inventory alerts for operations teams
-- Identifies products needing attention: low stock, overstock, stale inventory, high-value items

WITH inventory AS (
    SELECT * FROM {{ ref('silver_inventory') }}
),

-- Critical stock alerts (quantity = 0 or critical status)
critical_alerts AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,

        'Critical Stock' AS alert_type,
        'High' AS severity,
        'Immediate restocking required - out of stock or critically low' AS alert_message

    FROM inventory
    WHERE stock_status = 'Critical'
),

-- Low stock alerts (below reorder point but not critical)
low_stock_alerts AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,

        'Low Stock' AS alert_type,
        'Medium' AS severity,
        'Reorder recommended - quantity below reorder point' AS alert_message

    FROM inventory
    WHERE stock_status = 'Low' AND needs_reorder = TRUE
),

-- Overstock alerts (more than 2x reorder quantity)
overstock_alerts AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,

        'Overstock' AS alert_type,
        'Low' AS severity,
        'Excessive inventory - consider promotions or redistribution' AS alert_message

    FROM inventory
    WHERE stock_status = 'Overstock'
),

-- Stale inventory alerts (not restocked in 60+ days)
stale_inventory_alerts AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,

        'Stale Inventory' AS alert_type,
        'Medium' AS severity,
        'No restock in 60+ days - verify product demand' AS alert_message

    FROM inventory
    WHERE days_since_restock >= 60
),

-- High-value items needing attention (valuable products that need reordering)
high_value_alerts AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,

        'High-Value Item Alert' AS alert_type,
        'High' AS severity,
        'High-value product needs reorder - priority restocking' AS alert_message

    FROM inventory
    WHERE inventory_value > 1000 AND needs_reorder = TRUE
),

-- Union all alerts
all_alerts AS (
    SELECT * FROM critical_alerts
    UNION ALL
    SELECT * FROM low_stock_alerts
    UNION ALL
    SELECT * FROM overstock_alerts
    UNION ALL
    SELECT * FROM stale_inventory_alerts
    UNION ALL
    SELECT * FROM high_value_alerts
),

-- Add priority ranking
final AS (
    SELECT
        inventory_id,
        product_id,
        product_name,
        product_category,
        store_id,
        store_name,
        store_city,
        store_state,
        quantity_on_hand,
        reorder_point,
        inventory_value,
        days_since_restock,
        stock_status,
        alert_type,
        severity,
        alert_message,

        -- Priority score (1 = highest priority)
        ROW_NUMBER() OVER (
            ORDER BY
                CASE severity
                    WHEN 'High' THEN 1
                    WHEN 'Medium' THEN 2
                    WHEN 'Low' THEN 3
                END,
                inventory_value DESC,
                days_since_restock DESC
        ) AS priority_rank,

        CURRENT_TIMESTAMP AS alert_generated_at

    FROM all_alerts
)

SELECT * FROM final
ORDER BY priority_rank
