{{
  config(
    materialized='table',
    tags=['gold', 'funnel_analytics']
  )
}}

-- Gold layer: Customer conversion funnel analytics
-- Tracks customer journey from page views to purchases with conversion rates

WITH events AS (
    SELECT * FROM {{ ref('silver_customer_events') }}
),

-- Overall funnel metrics
overall_funnel AS (
    SELECT
        'Overall' AS dimension_type,
        'All' AS dimension_value,

        -- Funnel stages
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN event_id END) AS product_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN event_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN event_id END) AS checkout_started,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN event_id END) AS purchases,

        -- Unique users at each stage
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN customer_id END) AS unique_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN customer_id END) AS unique_product_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN customer_id END) AS unique_cart_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN customer_id END) AS unique_checkout_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN customer_id END) AS unique_buyers,

        -- Sessions
        COUNT(DISTINCT session_id) AS total_sessions

    FROM events
),

-- Device breakdown
device_funnel AS (
    SELECT
        'Device' AS dimension_type,
        device_category AS dimension_value,

        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN event_id END) AS product_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN event_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN event_id END) AS checkout_started,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN event_id END) AS purchases,

        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN customer_id END) AS unique_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN customer_id END) AS unique_product_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN customer_id END) AS unique_cart_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN customer_id END) AS unique_checkout_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN customer_id END) AS unique_buyers,

        COUNT(DISTINCT session_id) AS total_sessions

    FROM events
    GROUP BY device_category
),

-- Browser breakdown
browser_funnel AS (
    SELECT
        'Browser' AS dimension_type,
        browser AS dimension_value,

        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN event_id END) AS product_views,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN event_id END) AS add_to_cart,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN event_id END) AS checkout_started,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN event_id END) AS purchases,

        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'page_view' THEN customer_id END) AS unique_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'product_view' THEN customer_id END) AS unique_product_viewers,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'add_to_cart' THEN customer_id END) AS unique_cart_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'checkout' THEN customer_id END) AS unique_checkout_users,
        COUNT(DISTINCT CASE WHEN LOWER(event_type) = 'purchase' THEN customer_id END) AS unique_buyers,

        COUNT(DISTINCT session_id) AS total_sessions

    FROM events
    GROUP BY browser
),

-- Union all dimensions
all_funnels AS (
    SELECT * FROM overall_funnel
    UNION ALL
    SELECT * FROM device_funnel
    UNION ALL
    SELECT * FROM browser_funnel
),

-- Calculate conversion rates
final AS (
    SELECT
        dimension_type,
        dimension_value,

        -- Event counts
        page_views,
        product_views,
        add_to_cart,
        checkout_started,
        purchases,

        -- Unique user counts
        unique_viewers,
        unique_product_viewers,
        unique_cart_users,
        unique_checkout_users,
        unique_buyers,

        total_sessions,

        -- Conversion rates (event-based)
        ROUND(100.0 * purchases / NULLIF(page_views, 0), 2) AS overall_conversion_rate,
        ROUND(100.0 * product_views / NULLIF(page_views, 0), 2) AS page_to_product_rate,
        ROUND(100.0 * add_to_cart / NULLIF(product_views, 0), 2) AS product_to_cart_rate,
        ROUND(100.0 * checkout_started / NULLIF(add_to_cart, 0), 2) AS cart_to_checkout_rate,
        ROUND(100.0 * purchases / NULLIF(checkout_started, 0), 2) AS checkout_to_purchase_rate,

        -- Abandonment rates
        ROUND(100.0 * (add_to_cart - checkout_started) / NULLIF(add_to_cart, 0), 2) AS cart_abandonment_rate,
        ROUND(100.0 * (checkout_started - purchases) / NULLIF(checkout_started, 0), 2) AS checkout_abandonment_rate,

        -- User-based conversion rate
        ROUND(100.0 * unique_buyers / NULLIF(unique_viewers, 0), 2) AS user_conversion_rate,

        CURRENT_TIMESTAMP AS calculated_at

    FROM all_funnels
)

SELECT * FROM final
ORDER BY dimension_type, dimension_value
