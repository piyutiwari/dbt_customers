{{
  config(
    materialized='table',
    tags=['silver', 'products']
  )
}}

-- Silver layer: Cleaned and enriched product data
-- Applies business rules, calculations, and classifications

WITH source AS (
    SELECT * FROM {{ ref('bronze_products') }}
),

cleaned_products AS (
    SELECT
        product_id,

        -- Clean product names: trim whitespace and proper case
        TRIM(INITCAP(product_name)) AS product_name,

        -- Standardize category and subcategory
        TRIM(UPPER(category)) AS category,
        TRIM(INITCAP(subcategory)) AS subcategory,

        price,
        cost,

        -- Handle missing suppliers
        COALESCE(NULLIF(TRIM(supplier), ''), 'Unknown') AS supplier,

        -- Validate and standardize SKU
        UPPER(TRIM(sku)) AS sku,

        created_at,
        updated_at,

        -- Calculate profit margin: (price - cost) / price * 100
        CASE
            WHEN price > 0 THEN ROUND(((price - cost) / price * 100), 2)
            ELSE 0
        END AS profit_margin,

        -- Add category groupings
        CASE
            WHEN UPPER(category) IN ('ELECTRONICS', 'OFFICE SUPPLIES') THEN 'Tech & Office'
            WHEN UPPER(category) IN ('CLOTHING', 'ACCESSORIES') THEN 'Fashion & Accessories'
            WHEN UPPER(category) IN ('HOME & GARDEN', 'FURNITURE') THEN 'Home & Living'
            WHEN UPPER(category) IN ('SPORTS', 'OUTDOOR') THEN 'Sports & Outdoors'
            WHEN UPPER(category) IN ('BOOKS', 'MEDIA') THEN 'Books & Media'
            ELSE 'Other'
        END AS category_group,

        -- Price tier classification
        CASE
            WHEN price < 20 THEN 'Budget'
            WHEN price >= 20 AND price <= 75 THEN 'Mid'
            WHEN price > 75 THEN 'Premium'
            ELSE 'Unknown'
        END AS price_tier,

        CURRENT_TIMESTAMP AS processed_at

    FROM source
)

SELECT * FROM cleaned_products
