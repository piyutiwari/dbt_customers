{{
  config(
    materialized='table',
    tags=['gold', 'metrics', 'revenue']
  )
}}

-- Gold layer: Daily revenue metrics
-- Provides daily business performance metrics

SELECT
    order_date,
    COUNT(order_id) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    CURRENT_TIMESTAMP as aggregated_at
FROM {{ ref('silver_orders') }}
GROUP BY order_date
ORDER BY order_date DESC
