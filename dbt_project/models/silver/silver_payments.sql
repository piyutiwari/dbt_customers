{{
  config(
    materialized='table',
    tags=['silver', 'payments']
  )
}}

-- Silver layer: Cleaned and validated payment data
-- Joins with orders, calculates fees, flags suspicious transactions

WITH payments AS (
    SELECT * FROM {{ ref('bronze_payments') }}
),

orders AS (
    SELECT order_id FROM {{ ref('bronze_orders') }}
),

cleaned_payments AS (
    SELECT
        p.payment_id,
        p.order_id,

        -- Normalize payment method (lowercase, standardized)
        LOWER(TRIM(
            CASE
                WHEN LOWER(p.payment_method) IN ('cc', 'creditcard', 'credit_card') THEN 'credit_card'
                WHEN LOWER(p.payment_method) IN ('dc', 'debitcard', 'debit_card') THEN 'debit_card'
                WHEN LOWER(p.payment_method) = 'paypal' THEN 'paypal'
                WHEN LOWER(p.payment_method) IN ('applepay', 'apple_pay') THEN 'apple_pay'
                WHEN LOWER(p.payment_method) IN ('googlepay', 'google_pay') THEN 'google_pay'
                ELSE p.payment_method
            END
        )) AS payment_method,

        p.transaction_amount,
        p.processing_fee,
        p.payment_gateway,
        p.payment_status,
        p.transaction_date,

        -- Calculate net amount after processing fees
        p.transaction_amount - COALESCE(p.processing_fee, 0) AS net_amount,

        -- Calculate processing fee percentage
        CASE
            WHEN p.transaction_amount > 0 THEN ROUND((p.processing_fee / p.transaction_amount * 100), 2)
            ELSE 0
        END AS processing_fee_percentage,

        -- Payment success flag
        CASE
            WHEN LOWER(p.payment_status) IN ('completed', 'success', 'approved') THEN TRUE
            ELSE FALSE
        END AS is_successful,

        -- Flag suspicious transactions
        -- Suspicious if: processing fee > 10% OR status is 'failed' but amount > 1000 OR missing gateway
        CASE
            WHEN p.processing_fee > (p.transaction_amount * 0.10) THEN TRUE
            WHEN LOWER(p.payment_status) = 'failed' AND p.transaction_amount > 1000 THEN TRUE
            WHEN p.payment_gateway IS NULL OR TRIM(p.payment_gateway) = '' THEN TRUE
            ELSE FALSE
        END AS is_suspicious,

        -- Check if order exists (data quality flag)
        CASE
            WHEN o.order_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_valid_order,

        CURRENT_TIMESTAMP AS processed_at

    FROM payments p
    LEFT JOIN orders o ON p.order_id = o.order_id
)

SELECT * FROM cleaned_payments
