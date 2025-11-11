{{
  config(
    materialized='incremental',
    unique_key=['event_date', 'customer_id'],
    on_schema_change='append_new_columns',
    tags=['gold', 'customer_events', 'incremental', 'daily_aggregates']
  )
}}

-- Gold layer: Daily customer event aggregates (incremental)
-- Aggregates customer behavioral data by customer and day
-- Uses merge strategy to handle late-arriving data (processes last 7 days)

WITH events AS (
    SELECT * FROM {{ ref('silver_customer_events') }}
    {% if is_incremental() %}
    -- Process last 7 days to handle late-arriving data
    WHERE DATE(event_timestamp) >= DATEADD(day, -7, CURRENT_DATE)
    {% endif %}
),

daily_aggregates AS (
    SELECT
        customer_id,
        DATE(event_timestamp) AS event_date,

        -- Event counts by type
        COUNT(*) AS total_events,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN event_id END) AS product_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN event_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN event_id END) AS checkouts,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN event_id END) AS purchases,

        -- Session metrics
        COUNT(DISTINCT session_id) AS sessions_count,
        AVG(event_sequence_in_session) AS avg_events_per_session,

        -- Device and browser diversity
        COUNT(DISTINCT device_type) AS unique_devices_used,
        COUNT(DISTINCT browser) AS unique_browsers_used,
        MAX(device_category) AS primary_device_category,  -- Most common device category
        MAX(browser) AS primary_browser,  -- Most common browser

        -- Behavioral flags
        MAX(CASE WHEN is_conversion THEN 1 ELSE 0 END) AS had_conversion,
        MAX(CASE WHEN LOWER(event_type) = 'purchase' THEN 1 ELSE 0 END) AS made_purchase,

        -- Time metrics
        MIN(event_timestamp) AS first_event_time,
        MAX(event_timestamp) AS last_event_time,
        DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) AS total_active_minutes,

        -- Customer info (take any value since it's the same customer)
        MAX(customer_name) AS customer_name,
        MAX(customer_email) AS customer_email,

        CURRENT_TIMESTAMP AS calculated_at

    FROM events
    GROUP BY customer_id, DATE(event_timestamp)
)

SELECT * FROM daily_aggregates
