{{
  config(
    materialized='view',
    tags=['bronze', 'inventory']
  )
}}

-- Bronze layer: Raw inventory data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    inventory_id,
    product_id,
    store_id,
    quantity_on_hand,
    reorder_point,
    reorder_quantity,
    last_restocked_at,
    updated_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_inventory') }}
