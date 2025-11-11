{{
  config(
    materialized='table',
    tags=['gold', 'product_analytics']
  )
}}

-- Gold layer: Product performance metrics for business analytics
-- Aggregates revenue, profit, inventory data by product using actual order items data

WITH products AS (
    SELECT * FROM {{ ref('silver_products') }}
),

order_items AS (
    SELECT * FROM {{ ref('silver_order_items') }}
),

inventory AS (
    SELECT
        product_id,
        SUM(inventory_value) AS total_inventory_value,
        COUNT(DISTINCT store_id) AS stores_carrying_product,
        SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) AS stores_needing_reorder
    FROM {{ ref('silver_inventory') }}
    GROUP BY product_id
),

-- Aggregate sales metrics by product
product_sales AS (
    SELECT
        product_id,

        -- Revenue metrics
        SUM(net_line_total) AS total_revenue,
        SUM(quantity) AS total_units_sold,
        AVG(unit_price) AS average_selling_price,

        -- Profit metrics
        SUM(line_profit) AS total_profit,
        AVG(line_profit_margin_pct) AS avg_profit_margin_pct,

        -- Discount metrics
        SUM(discount) AS total_discounts_given,
        AVG(discount_pct) AS avg_discount_pct,

        -- Order metrics
        COUNT(DISTINCT order_id) AS orders_containing_product,
        COUNT(DISTINCT customer_id) AS unique_customers,

        -- Date metrics
        MIN(order_date) AS first_sale_date,
        MAX(order_date) AS last_sale_date,
        DATEDIFF(day, MAX(order_date), CURRENT_DATE) AS days_since_last_sale

    FROM order_items
    GROUP BY product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.category_group,
        p.price_tier,
        p.price AS current_catalog_price,
        p.cost AS current_cost,
        p.profit_margin AS current_catalog_profit_margin,
        p.supplier,

        -- Inventory metrics
        COALESCE(i.total_inventory_value, 0) AS total_inventory_value,
        COALESCE(i.stores_carrying_product, 0) AS stores_carrying_product,
        COALESCE(i.stores_needing_reorder, 0) AS stores_needing_reorder,

        -- Sales metrics (actual data from order_items)
        COALESCE(s.total_revenue, 0) AS total_revenue,
        COALESCE(s.total_units_sold, 0) AS total_units_sold,
        COALESCE(s.average_selling_price, 0) AS average_selling_price,
        COALESCE(s.total_profit, 0) AS total_profit,
        COALESCE(s.avg_profit_margin_pct, 0) AS avg_profit_margin_pct,

        -- Discount metrics
        COALESCE(s.total_discounts_given, 0) AS total_discounts_given,
        COALESCE(s.avg_discount_pct, 0) AS avg_discount_pct,

        -- Order metrics
        COALESCE(s.orders_containing_product, 0) AS orders_containing_product,
        COALESCE(s.unique_customers, 0) AS unique_customers,

        -- Performance indicators
        CASE
            WHEN s.total_units_sold > 0
            THEN ROUND(s.total_revenue / s.total_units_sold, 2)
            ELSE 0
        END AS revenue_per_unit,

        CASE
            WHEN s.total_units_sold > 0
            THEN ROUND(s.total_profit / s.total_units_sold, 2)
            ELSE 0
        END AS profit_per_unit,

        -- Sales velocity (units sold per day since first sale)
        CASE
            WHEN s.first_sale_date IS NOT NULL AND DATEDIFF(day, s.first_sale_date, CURRENT_DATE) > 0
            THEN ROUND(s.total_units_sold::FLOAT / DATEDIFF(day, s.first_sale_date, CURRENT_DATE), 2)
            ELSE 0
        END AS avg_units_sold_per_day,

        -- Date metrics
        s.first_sale_date,
        s.last_sale_date,
        COALESCE(s.days_since_last_sale, NULL) AS days_since_last_sale,

        -- Product status flags
        CASE WHEN s.total_units_sold IS NULL THEN TRUE ELSE FALSE END AS never_sold,
        CASE WHEN s.days_since_last_sale > 90 THEN TRUE ELSE FALSE END AS inactive_product,

        CURRENT_TIMESTAMP AS calculated_at

    FROM products p
    LEFT JOIN inventory i ON p.product_id = i.product_id
    LEFT JOIN product_sales s ON p.product_id = s.product_id
)

SELECT * FROM final
ORDER BY total_revenue DESC
