WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)
SELECT
    product_id,
    title,
    category,
    brand,
    price,
    discount_percentage,
    rating,
    stock,
    availability_status,
FROM products