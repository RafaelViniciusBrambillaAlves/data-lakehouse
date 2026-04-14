{{ config(
    materialized = 'view'
) }}

WITH ratings AS (

    SELECT * FROM {{ ref('stg_ratings') }}

),

genres AS (

    SELECT * FROM {{ 'stg_movie_genres' }}

),

joined AS (

    SELECT 
        g.genre,
        r.rating
    FROM ratings AS r
    INNER JOIN genres AS g
        ON r.movie_id = g.movie_id

),

aggregated AS (

    SELECT 
        genre,

        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating

    FROM joined
    GROUP BY genre

)

SELECT * FROM aggregated