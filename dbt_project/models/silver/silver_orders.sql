{{
  config(
    materialized='table',
    tags=['silver', 'orders']
  )
}}

-- Silver layer: Cleaned and enriched order data
-- Applies data quality rules and business logic

WITH cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        amount,
        -- Apply business rules to validate orders
        CASE
            WHEN amount > 0 AND order_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END as is_valid_order,
        CURRENT_TIMESTAMP as processed_at
    FROM {{ ref('bronze_orders') }}
    WHERE
        -- Filter out invalid records
        order_id IS NOT NULL
        AND customer_id IS NOT NULL
)

SELECT * FROM cleaned_orders
WHERE is_valid_order = TRUE  -- Only keep valid orders
