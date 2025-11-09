{{
  config(
    materialized='table',
    tags=['silver', 'customers']
  )
}}

-- Silver layer: Cleaned and standardized customer data
-- Applies data quality rules, standardization, and enrichment

WITH cleaned_customers AS (
    SELECT
        customer_id,
        -- Standardize customer name (trim whitespace, proper case)
        INITCAP(TRIM(customer_name)) as customer_name,
        -- Standardize email (lowercase, trim)
        LOWER(TRIM(email)) as email,
        -- Extract email domain for segmentation
        SPLIT_PART(LOWER(TRIM(email)), '@', 2) as email_domain,
        created_at,
        -- Determine if customer is active (has activity in last 90 days)
        CASE
            WHEN created_at >= CURRENT_DATE - INTERVAL '90 days' THEN TRUE
            ELSE FALSE
        END as is_active,
        CURRENT_TIMESTAMP as processed_at
    FROM {{ ref('bronze_customers') }}
    WHERE
        -- Filter out invalid records
        customer_id IS NOT NULL
        AND email IS NOT NULL
        AND email LIKE '%@%'  -- Basic email validation
)

SELECT * FROM cleaned_customers
