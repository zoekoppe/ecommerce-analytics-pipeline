WITH users AS (
    SELECT * FROM {{ source('thelook', 'users') }}
)
SELECT
    id as user_id,
    first_name,
    last_name,
    email,
    age,
    gender,
    state,
    country,
    traffic_source,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM users