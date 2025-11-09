{{
  config(
    materialized='view',
    tags=['bronze', 'events']
  )
}}

-- Bronze layer: Raw customer event data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    event_id,
    customer_id,
    event_type,
    event_category,
    page_url,
    session_id,
    device_type,
    browser,
    event_timestamp,
    created_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_customer_events') }}
