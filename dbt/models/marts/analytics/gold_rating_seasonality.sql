{{ config(
    materialized = 'table'
) }}

WITH ratings AS (

    SELECT * FROM {{ ref('stg_ratings') }}

),

features AS (

    SELECT 
        rating, 
        event_timestamp,

        EXTRACT(month FROM event_timestamp) AS month,

        EXTRACT(dow FROM event_timestamp) AS day_of_week

    FROM ratings

),

aggregated AS (

    SELECT 
        month,
        day_of_week,

        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating

    FROM features

    GROUP BY month, day_of_week

)

SELECT * 
FROM aggregated
ORDER BY month, day_of_week

