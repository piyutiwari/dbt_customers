{{
  config(
    materialized='view',
    tags=['bronze', 'payments']
  )
}}

-- Bronze layer: Raw payment data with minimal transformation
-- This model reads from the raw source and adds metadata

SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    transaction_amount,
    processing_fee,
    gateway,
    card_last_four,
    processed_at,
    updated_at,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw_data', 'raw_payments') }}
