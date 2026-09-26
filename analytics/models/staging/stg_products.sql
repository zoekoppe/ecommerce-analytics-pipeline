WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
)
,latest AS (
    SELECT * FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id 
                ORDER BY ingested_at DESC) = 1
)
,renamed AS (
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
    FROM latest
)
SELECT * FROM renamed