WITH source AS (
    SELECT * FROM {{ source('thelook', 'order_items') }}
),
renamed AS (
    SELECT
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        status,
        CAST(created_at AS TIMESTAMP) AS created_at,
        sale_price
    FROM source
)
SELECT * FROM renamed