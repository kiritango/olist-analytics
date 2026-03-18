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

{{ config(
    engine='MergeTree()',
    order_by='(order_id, product_id)'
) }}