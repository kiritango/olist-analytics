{{ config(
    engine='MergeTree()',
    order_by='(users)'
) }}

SELECT
    stage_order,
    stage,
    users,
    round(users / max(users) OVER () * 100, 2) AS conversion_pct
FROM (
    SELECT 1 AS stage_order, 'created'   AS stage, count(DISTINCT order_id) AS users
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT 2, 'approved', countIf(approved_at IS NOT NULL)
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT 3, 'shipped', countIf(shipped_at IS NOT NULL)
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT 4, 'delivered', countIf(delivered_at IS NOT NULL)
    FROM {{ ref('stg_orders') }}
)