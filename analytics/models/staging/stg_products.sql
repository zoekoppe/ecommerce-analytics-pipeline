WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),
renamed AS (
    SELECT
        product_id,
        title,
        category,
        brand,
        price,
        discount_percentage,
        rating,
        stock,
        sku,
        availability_status,
        ingested_at
    FROM source
)
SELECT * FROM renamed