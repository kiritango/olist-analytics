{{ config(
    engine='MergeTree()',
    order_by='(day)'
) }}

SELECT
    toDate(ordered_at)          AS day,
    uniq(customer_id)           AS dau,
    count(DISTINCT order_id)    AS orders
FROM {{ ref('stg_orders') }}
GROUP BY day
ORDER BY day