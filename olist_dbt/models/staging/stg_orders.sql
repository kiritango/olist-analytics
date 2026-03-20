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

SELECT
    order_id,
    customer_id,
    order_status,
    parseDateTimeBestEffort(order_purchase_timestamp)               AS ordered_at,
    if(order_approved_at != '',
       parseDateTimeBestEffort(order_approved_at), NULL)            AS approved_at,
    if(order_delivered_carrier_date != '',
       parseDateTimeBestEffort(order_delivered_carrier_date), NULL) AS shipped_at,
    if(order_delivered_customer_date != '',
       parseDateTimeBestEffort(order_delivered_customer_date), NULL) AS delivered_at,
    if(order_estimated_delivery_date != '',
       parseDateTimeBestEffort(order_estimated_delivery_date), NULL) AS estimated_at,
    if(order_purchase_timestamp != '' AND order_delivered_customer_date != '',
       dateDiff('day',
           parseDateTimeBestEffort(order_purchase_timestamp),
           parseDateTimeBestEffort(order_delivered_customer_date)),
       NULL)                                                        AS delivery_days
FROM {{ source('raw', 'orders_stream') }}
WHERE order_purchase_timestamp IS NOT NULL
  AND order_purchase_timestamp != ''
  AND order_status NOT IN ('unavailable', 'canceled')