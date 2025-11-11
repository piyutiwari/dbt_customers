{{
  config(
    materialized='table',
    tags=['silver', 'customer_events']
  )
}}

-- Silver layer: Cleaned and enriched customer behavioral event data
-- Enriches with customer info, extracts URL domains, classifies devices and events

WITH events AS (
    SELECT * FROM {{ ref('bronze_customer_events') }}
),

customers AS (
    SELECT
        customer_id,
        customer_name,
        email
    FROM {{ ref('silver_customers') }}
),

enriched_events AS (
    SELECT
        e.event_id,
        e.customer_id,
        e.session_id,
        e.event_type,
        e.event_timestamp,
        e.page_url,
        e.device_type,
        e.browser,

        -- Enrichment from customers
        c.customer_name,
        c.email AS customer_email,

        -- Extract domain from page_url
        CASE
            WHEN e.page_url LIKE 'http://%' THEN SPLIT_PART(SUBSTRING(e.page_url, 8), '/', 1)
            WHEN e.page_url LIKE 'https://%' THEN SPLIT_PART(SUBSTRING(e.page_url, 9), '/', 1)
            ELSE SPLIT_PART(e.page_url, '/', 1)
        END AS url_domain,

        -- Classify device category
        CASE
            WHEN LOWER(e.device_type) IN ('mobile', 'tablet', 'smartphone') THEN 'Mobile'
            WHEN LOWER(e.device_type) IN ('desktop', 'laptop', 'pc') THEN 'Desktop'
            ELSE 'Other'
        END AS device_category,

        -- Flag conversion events (purchase, checkout, add_to_cart)
        CASE
            WHEN LOWER(e.event_type) IN ('purchase', 'checkout', 'conversion', 'order_completed') THEN TRUE
            ELSE FALSE
        END AS is_conversion,

        -- Calculate event sequence within session
        ROW_NUMBER() OVER (PARTITION BY e.session_id ORDER BY e.event_timestamp) AS event_sequence_in_session,

        CURRENT_TIMESTAMP AS processed_at

    FROM events e
    LEFT JOIN customers c ON e.customer_id = c.customer_id
)

SELECT * FROM enriched_events
