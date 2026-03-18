SELECT
    order_id,
    payment_type,
    payment_value
FROM {{ source('raw', 'payments') }}
WHERE payment_value > 0
  AND payment_type != 'not_defined'

{{ config(
    engine='MergeTree()',
    order_by='(order_id)'
) }}