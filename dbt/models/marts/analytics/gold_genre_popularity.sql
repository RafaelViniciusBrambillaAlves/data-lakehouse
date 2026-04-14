{{ config(
    materialized = 'table'
) }}

WITH ratings AS (

    SELECT * FROM {{ ref('stg_ratings') }}

),

genres AS (

    SELECT * FROM {{ ref('stg_movie_genres') }}

),

joined AS (

    SELECT 
        g.genre,
        r.rating

    FROM ratings AS r
    INNER JOIN genres g 
        ON r.movie_id = g.movie_id

),

aggregated AS (

    SELECT
        genre,
        
        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_ratings
    
    FROM joined 
    GROUP BY genre

)

SELECT *
FROM aggregated
ORDER BY avg_ratings DESC, total_ratings DESC
