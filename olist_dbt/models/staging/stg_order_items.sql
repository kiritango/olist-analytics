{{ config(
    engine='MergeTree()',
    order_by='(order_id, product_id)'
) }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    price + freight_value AS total_price
FROM {{ source('raw', 'order_items') }}
WHERE price > 0

UNION ALL

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    parseDateTimeBestEffort(shipping_limit_date) AS shipping_limit_date,
    toDecimal64(price, 2) AS price,
    toDecimal64(freight_value, 2) AS freight_value,
    toDecimal64(price + freight_value, 2) AS total_price
FROM {{ source('raw', 'order_items_stream') }}
WHERE price > 0