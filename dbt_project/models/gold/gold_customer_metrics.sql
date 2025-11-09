{{
  config(
    materialized='table',
    tags=['gold', 'metrics', 'customers']
  )
}}

-- Gold layer: Customer-level business metrics
-- Aggregates data to provide business insights and KPIs

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        c.email_domain,
        c.is_active,
        COUNT(o.order_id) as total_orders,
        COALESCE(SUM(o.amount), 0) as total_revenue,
        COALESCE(AVG(o.amount), 0) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM {{ ref('silver_customers') }} c
    LEFT JOIN {{ ref('silver_orders') }} o
        ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_id,
        c.customer_name,
        c.email,
        c.email_domain,
        c.is_active
)

SELECT
    customer_id,
    customer_name,
    email,
    email_domain,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    -- Calculate customer lifetime in days
    CASE
        WHEN first_order_date IS NOT NULL
        THEN EXTRACT(DAY FROM (CURRENT_DATE - first_order_date))
        ELSE 0
    END as customer_lifetime_days,
    is_active,
    CURRENT_TIMESTAMP as aggregated_at
FROM customer_orders
