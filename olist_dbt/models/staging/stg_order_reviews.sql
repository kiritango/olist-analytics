{{ config(
    engine='MergeTree()',
    order_by='(order_id, created_at)'
) }}

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title        AS title,
    review_comment_message      AS message,
    review_creation_date        AS created_at,
    review_answer_timestamp     AS answered_at
FROM {{ source('raw', 'order_reviews') }}
WHERE review_id IS NOT NULL
  AND order_id IS NOT NULL