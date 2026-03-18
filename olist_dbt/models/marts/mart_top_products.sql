{{ config(
    engine='MergeTree()',
    order_by='(category)' 
) }}

SELECT
    row_number() OVER (ORDER BY sum(i.price) DESC) AS rank,
    p.category,
    count(DISTINCT i.order_id)              AS orders,
    sum(i.price)                            AS revenue,
    round(avg(i.price), 2)                  AS avg_price,
    round(sum(i.price) / sum(sum(i.price))
        OVER () * 100, 2)                   AS revenue_share_pct
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('stg_products') }} p USING (product_id)
WHERE p.category IS NOT NULL
GROUP BY p.category
LIMIT 20