{{ config(
    engine='MergeTree()',
    order_by='(product_id)'
) }}

SELECT
    product_id,
    coalesce(product_category_name, 'unknown') AS category,
    product_name_lenght             AS name_length,
    product_description_lenght      AS description_length,
    product_photos_qty              AS photos_qty,
    product_weight_g                AS weight_g
FROM {{ source('raw', 'products') }}
WHERE product_id IS NOT NULL