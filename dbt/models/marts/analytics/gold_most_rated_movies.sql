{{ config(
    materialized = 'table',
) }}

with movie_metrics AS (

    SELECT * FROM {{ ref("int_movie_metrics") }}

)

SELECT * 
FROM movie_metrics
ORDER BY total_ratings DESC
LIMIT 10