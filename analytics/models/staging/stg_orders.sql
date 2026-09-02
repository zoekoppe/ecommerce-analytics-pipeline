WITH source AS (
    SELECT * FROM {{ source('thelook', 'orders') }}
),
renamed AS (
    SELECT
        order_id,
        user_id,
        status,
        CAST(created_at AS TIMESTAMP) AS created_at,
        num_of_item AS item_count
    FROM source
)
SELECT * FROM renamed