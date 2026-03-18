{{ config(
    engine='MergeTree()',
    order_by='(order_id, ordered_at)',
    partition_by='toYYYYMM(ordered_at)'
) }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp            AS ordered_at,
    order_approved_at                   AS approved_at,
    order_delivered_carrier_date        AS shipped_at,
    order_delivered_customer_date       AS delivered_at,
    order_estimated_delivery_date       AS estimated_at,
    dateDiff('day',
        order_purchase_timestamp,
        order_delivered_customer_date)  AS delivery_days
FROM {{ source('raw', 'orders') }}
WHERE order_purchase_timestamp IS NOT NULL
  AND order_status NOT IN ('unavailable', 'canceled')