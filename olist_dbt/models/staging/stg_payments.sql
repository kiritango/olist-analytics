{{ config(
    engine='MergeTree()',
    order_by='(order_id)'
) }}

SELECT
    order_id,
    payment_type,
    payment_value
FROM {{ source('raw', 'payments') }}
WHERE payment_value > 0
  AND payment_type != 'not_defined'

UNION ALL

SELECT
    order_id,
    payment_type,
    toDecimal64(payment_value, 2) AS payment_value
FROM {{ source('raw', 'payments_stream') }}
WHERE payment_value > 0
  AND payment_type != 'not_defined'