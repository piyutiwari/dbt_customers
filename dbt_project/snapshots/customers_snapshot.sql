{% snapshot customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    strategy='timestamp',
    unique_key='customer_id',
    updated_at='created_at'
  )
}}

-- Snapshot of customer data to track changes over time
-- Tracks: email changes, name changes, status changes
-- Use cases: customer audit trail, data quality monitoring
-- Note: Using created_at as updated_at timestamp. Consider adding an actual updated_at field to the source for better change tracking.

SELECT * FROM {{ source('raw_data', 'raw_customers') }}

{% endsnapshot %}
