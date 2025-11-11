{{
  config(
    materialized='view',
    tags=['bronze', 'order_items']
  )
}}

-- Bronze layer: Raw order item data with minimal transformation
-- This model reads from the raw source and adds metadata
-- Links orders to products with quantity and pricing details

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    line_total,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_order_items') }}
