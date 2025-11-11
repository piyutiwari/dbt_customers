{{
  config(
    materialized='table',
    tags=['silver', 'order_items']
  )
}}

-- Silver layer: Cleaned and enriched order item data
-- Validates pricing, calculates margins, enriches with product and order details

WITH order_items AS (
    SELECT * FROM {{ ref('bronze_order_items') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category,
        category_group,
        price AS current_price,
        cost AS current_cost,
        profit_margin AS current_profit_margin
    FROM {{ ref('silver_products') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM {{ ref('silver_orders') }}
),

enriched_order_items AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.line_total,

        -- Enrichment from products
        p.product_name,
        p.category AS product_category,
        p.category_group,
        p.current_cost AS product_cost,

        -- Enrichment from orders
        o.customer_id,
        o.order_date,

        -- Calculated fields
        oi.quantity * oi.unit_price AS gross_line_total,
        oi.quantity * oi.unit_price - oi.discount AS net_line_total,

        -- Verify line_total matches calculation (data quality check)
        CASE
            WHEN ABS(oi.line_total - (oi.quantity * oi.unit_price - oi.discount)) < 0.01 THEN TRUE
            ELSE FALSE
        END AS line_total_is_valid,

        -- Calculate item-level profit
        (oi.quantity * oi.unit_price - oi.discount) - (oi.quantity * p.current_cost) AS line_profit,

        -- Calculate profit margin for this line item
        CASE
            WHEN (oi.quantity * oi.unit_price - oi.discount) > 0
            THEN ROUND(((oi.quantity * oi.unit_price - oi.discount) - (oi.quantity * p.current_cost)) /
                      (oi.quantity * oi.unit_price - oi.discount) * 100, 2)
            ELSE 0
        END AS line_profit_margin_pct,

        -- Discount percentage
        CASE
            WHEN oi.quantity * oi.unit_price > 0
            THEN ROUND(oi.discount / (oi.quantity * oi.unit_price) * 100, 2)
            ELSE 0
        END AS discount_pct,

        -- Price comparison (was the price different from current catalog price?)
        CASE
            WHEN ABS(oi.unit_price - p.current_price) > 0.01 THEN TRUE
            ELSE FALSE
        END AS price_different_from_catalog,

        CURRENT_TIMESTAMP AS processed_at

    FROM order_items oi
    LEFT JOIN products p ON oi.product_id = p.product_id
    LEFT JOIN orders o ON oi.order_id = o.order_id
)

SELECT * FROM enriched_order_items
