{{
  config(
    materialized='table',
    tags=['silver', 'stores']
  )
}}

-- Silver layer: Cleaned and enriched store location data
-- Standardizes addresses, calculates store age, adds regional classifications

WITH source AS (
    SELECT * FROM {{ ref('bronze_stores') }}
),

cleaned_stores AS (
    SELECT
        store_id,

        -- Standardize store name
        TRIM(INITCAP(store_name)) AS store_name,

        -- Standardize address components
        TRIM(INITCAP(address)) AS address,
        TRIM(INITCAP(city)) AS city,
        UPPER(TRIM(state)) AS state,
        TRIM(zip_code) AS zip_code,

        -- Create full address
        TRIM(INITCAP(address)) || ', ' ||
        TRIM(INITCAP(city)) || ', ' ||
        UPPER(TRIM(state)) || ' ' ||
        TRIM(zip_code) AS full_address,

        -- Standardize phone format (remove non-numeric characters)
        REGEXP_REPLACE(phone, '[^0-9]', '') AS phone,

        store_type,
        opened_at,

        -- Calculate store age in days
        DATEDIFF(day, opened_at, CURRENT_DATE) AS store_age_days,

        -- Classify stores by age
        CASE
            WHEN DATEDIFF(day, opened_at, CURRENT_DATE) < 365 THEN 'New'
            WHEN DATEDIFF(day, opened_at, CURRENT_DATE) BETWEEN 365 AND 1095 THEN 'Established'
            WHEN DATEDIFF(day, opened_at, CURRENT_DATE) > 1095 THEN 'Mature'
            ELSE 'Unknown'
        END AS store_age_category,

        -- Add region classification based on state
        CASE
            WHEN UPPER(TRIM(state)) IN ('WA', 'OR', 'CA', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM', 'AK', 'HI') THEN 'West'
            WHEN UPPER(TRIM(state)) IN ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY', 'FL', 'GA', 'SC', 'NC', 'VA', 'WV') THEN 'South'
            WHEN UPPER(TRIM(state)) IN ('ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO', 'WI', 'IL', 'IN', 'MI', 'OH') THEN 'Central'
            WHEN UPPER(TRIM(state)) IN ('PA', 'NY', 'NJ', 'CT', 'RI', 'MA', 'VT', 'NH', 'ME', 'DE', 'MD', 'DC') THEN 'East'
            ELSE 'Other'
        END AS region,

        CURRENT_TIMESTAMP AS processed_at

    FROM source
)

SELECT * FROM cleaned_stores
