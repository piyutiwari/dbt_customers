{% snapshot products_snapshot %}

{{
  config(
    target_schema='snapshots',
    strategy='timestamp',
    unique_key='product_id',
    updated_at='updated_at'
  )
}}

-- Snapshot of product data to track historical changes
-- Tracks: price changes, cost changes, supplier changes, category changes
-- Use cases: historical price analysis, profit margin trends, supplier relationship history

SELECT * FROM {{ source('raw_data', 'raw_products') }}

{% endsnapshot %}
