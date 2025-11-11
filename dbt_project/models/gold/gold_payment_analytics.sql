{{
  config(
    materialized='table',
    tags=['gold', 'payment_analytics']
  )
}}

-- Gold layer: Payment method analytics for business intelligence
-- Aggregates transaction volumes, fees, success rates by payment method

WITH payments AS (
    SELECT * FROM {{ ref('silver_payments') }}
),

payment_method_stats AS (
    SELECT
        payment_method,

        -- Transaction volumes
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS failed_transactions,
        SUM(CASE WHEN LOWER(payment_status) = 'refunded' THEN 1 ELSE 0 END) AS refunded_transactions,

        -- Financial metrics
        SUM(transaction_amount) AS total_transaction_volume,
        SUM(CASE WHEN is_successful THEN transaction_amount ELSE 0 END) AS successful_transaction_volume,
        SUM(processing_fee) AS total_fees_paid,
        SUM(net_amount) AS total_net_amount,

        -- Averages
        AVG(transaction_amount) AS avg_transaction_amount,
        AVG(processing_fee) AS avg_processing_fee,
        AVG(processing_fee_percentage) AS avg_processing_fee_percentage,

        -- Success and failure rates
        ROUND(100.0 * SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN LOWER(payment_status) = 'refunded' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS refund_rate_pct,

        -- Suspicious transactions
        SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) AS suspicious_transaction_count,
        ROUND(100.0 * SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END) / COUNT(*), 2) AS suspicious_rate_pct,

        -- Date ranges
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date

    FROM payments
    GROUP BY payment_method
),

gateway_stats AS (
    SELECT
        payment_gateway,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_successful THEN transaction_amount ELSE 0 END) AS total_revenue,
        AVG(processing_fee_percentage) AS avg_fee_percentage,
        ROUND(100.0 * SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
    FROM payments
    WHERE payment_gateway IS NOT NULL
    GROUP BY payment_gateway
),

monthly_trends AS (
    SELECT
        payment_method,
        DATE_TRUNC('month', transaction_date) AS month,
        COUNT(*) AS transactions,
        SUM(CASE WHEN is_successful THEN transaction_amount ELSE 0 END) AS revenue,
        ROUND(100.0 * SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
    FROM payments
    GROUP BY payment_method, DATE_TRUNC('month', transaction_date)
),

final AS (
    SELECT
        pms.*,

        -- Add overall metrics for context
        ROUND(100.0 * pms.total_transaction_volume / SUM(pms.total_transaction_volume) OVER (), 2) AS pct_of_total_volume,
        ROUND(100.0 * pms.total_transactions / SUM(pms.total_transactions) OVER (), 2) AS pct_of_total_transactions,

        CURRENT_TIMESTAMP AS calculated_at

    FROM payment_method_stats pms
)

SELECT * FROM final
ORDER BY total_transaction_volume DESC
