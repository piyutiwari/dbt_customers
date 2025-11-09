{{
  config(
    materialized='view',
    tags=['bronze', 'products']
  )
}}

-- Bronze layer: Raw product data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    product_id,
    product_name,
    category,
    subcategory,
    price,
    cost,
    supplier,
    sku,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_products') }}
