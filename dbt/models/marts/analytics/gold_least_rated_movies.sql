{{ config ( 
    materialized = 'table',
) }}

WITH movie_metrics AS(

    SELECT * FROM {{ ref('int_movie_metrics') }}

)

SELECT *
FROM movie_metrics 
ORDER BY total_ratings ASC
LIMIT 10