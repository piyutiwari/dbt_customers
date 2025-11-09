{{
  config(
    materialized='view',
    tags=['bronze', 'orders']
  )
}}

-- Bronze layer: Raw order data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_orders') }}
