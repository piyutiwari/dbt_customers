{{
  config(
    materialized='view',
    tags=['bronze', 'stores']
  )
}}

-- Bronze layer: Raw store data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    store_id,
    store_name,
    store_type,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    manager_name,
    opened_at,
    updated_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_stores') }}
