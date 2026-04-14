{{ config(
    materialized = 'view'
) }}

WITH ratings AS(

    SELECT * FROM {{ ref('stg_ratings') }}

),

aggregated AS (

    SELECT 
        user_id,

        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating,

        AVG(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS pct_positive,

        CAST(MAX(event_timestamp) AS TIMESTAMP) AS last_activity

    FROM ratings
    GROUP BY user_id

)

SELECT * FROM aggregated 