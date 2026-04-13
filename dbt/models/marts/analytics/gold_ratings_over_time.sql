{{ config(
    materialized = 'table',
) }}

WITH base AS(

    SELECT * FROM {{ ref('stg_ratings') }}

),

aggregated AS (

    SELECT  
        DATE(event_timestamp) AS event_date,

        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating,
        AVG(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS pct_positive

    FROM base

    GROUP BY DATE(event_timestamp)

)

SELECT * FROM aggregated