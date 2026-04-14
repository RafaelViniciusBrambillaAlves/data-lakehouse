{{ config(
    materialized = 'table'
) }}

WITH base AS (

    SELECT * FROM {{ ref('int_movie_metrics') }}

),

filtered AS (

    SELECT 
        movie_id,
        title,
        avg_rating,
        total_ratings
 
    FROM base
 
    WHERE avg_rating >= 2
      AND total_ratings < 100

)

SELECT * 
FROM filtered 
ORDER BY avg_rating DESC, total_ratings ASC
