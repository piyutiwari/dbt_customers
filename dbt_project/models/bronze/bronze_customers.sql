{{
  config(
    materialized='view',
    tags=['bronze', 'customers']
  )
}}

-- Bronze layer: Raw data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    customer_id,
    customer_name,
    email,
    created_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_customers') }}
