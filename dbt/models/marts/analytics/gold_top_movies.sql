{{ config(
    materialized = 'table',
) }}

WITH movie_metrics AS(

    SELECT * FROM {{ ref('int_movie_metrics') }}

)

SELECT * 
FROM movie_metrics
ORDER BY avg_rating DESC, total_ratings DESC
LIMIT 10 