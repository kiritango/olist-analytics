{{ config(
    engine='MergeTree()',
    order_by='(cohort_month, order_month)'
) }}

WITH first_orders AS (
    SELECT
        customer_id,
        toStartOfMonth(min(ordered_at)) AS cohort_month
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT
    f.cohort_month,
    toStartOfMonth(o.ordered_at)                          AS order_month,
    uniq(o.customer_id)                             AS users,
    dateDiff('month', f.cohort_month, toStartOfMonth(o.ordered_at))         AS months_since_first
FROM {{ ref('stg_orders') }} o
JOIN first_orders f USING (customer_id)
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month