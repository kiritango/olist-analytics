{{ config(
    engine='MergeTree()',
    order_by='(order_id, ordered_at)',
    partition_by='toYYYYMM(ordered_at)'
) }}

-- Исторические данные из CSV
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

UNION ALL

-- Живые данные из Kafka → Spark
SELECT
    order_id,
    customer_id,
    order_status,
    parseDateTimeBestEffort(order_purchase_timestamp)   AS ordered_at,
    parseDateTimeBestEffort(order_approved_at)          AS approved_at,
    NULL                                                AS shipped_at,
    NULL                                                AS delivered_at,
    NULL                                                AS estimated_at,
    NULL                                                AS delivery_days
FROM {{ source('raw', 'orders_stream') }}
WHERE order_purchase_timestamp IS NOT NULL
  AND order_status NOT IN ('unavailable', 'canceled')