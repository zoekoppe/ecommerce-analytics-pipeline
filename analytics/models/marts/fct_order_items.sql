{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_item_id'
    )
}}

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
,joined AS (
    SELECT 
        items.order_item_id,
        items.order_id,
        items.product_id,
        items.user_id,
        orders.status,
        orders.created_at,
        items.sale_price
    FROM items
    LEFT JOIN orders using (order_id)
)

SELECT * FROM joined

-- incremental filter
{% if is_incremental() %}
WHERE created_at >= (
    SELECT TIMESTAMP_SUB(MAX(created_at), INTERVAL {{
        var('incremental_lookback_days', 3) }} DAY)
    FROM {{ this }}
)
{% endif %}