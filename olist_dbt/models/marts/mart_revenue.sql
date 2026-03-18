{{ config(
    engine='MergeTree()',
    order_by='(month)'
) }}

SELECT
    toStartOfMonth(ordered_at) AS month,
    count(DISTINCT o.order_id)      AS orders,
    sum(p.payment_value)            AS revenue,
    round(avg(p.payment_value), 2)  AS avg_check,
    round(sum(p.payment_value) /
          uniq(o.customer_id), 2)   AS arpu
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_payments') }} p USING (order_id)
GROUP BY month
ORDER BY month