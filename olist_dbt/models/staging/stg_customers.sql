SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ source('raw', 'customers') }}
WHERE customer_id IS NOT NULL

{{ config(
    engine='MergeTree()',
    order_by='(customer_id)'
) }}