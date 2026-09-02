WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
SELECT
    items.order_item_id,
    items.order_id,
    items.product_id,
    items.user_id,
    orders.status,
    orders.created_at,
    items.sale_price
FROM items
LEFT JOIN orders USING (order_id)