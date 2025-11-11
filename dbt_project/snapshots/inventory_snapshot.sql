{% snapshot inventory_snapshot %}

{{
  config(
    target_schema='snapshots',
    strategy='timestamp',
    unique_key='inventory_id',
    updated_at='updated_at'
  )
}}

-- Snapshot of inventory data to track changes over time
-- Tracks: inventory level changes, restock events, reorder point adjustments
-- Use cases: stock level trends, restock pattern analysis, inventory velocity metrics

SELECT * FROM {{ source('raw_data', 'raw_inventory') }}

{% endsnapshot %}
