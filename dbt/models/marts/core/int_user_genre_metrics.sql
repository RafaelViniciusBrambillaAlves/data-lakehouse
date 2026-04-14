{{ config(
    materialized = 'view'
) }}

WITH ratings AS(

    SELECT * FROM {{ ref('stg_ratings') }}

),

genres AS (

    SELECT * FROM {{ ref('stg_movie_genres') }}

),

joined AS (

    SELECT 
        r.user_Id,
        g.genre,
        r.rating

    FROM ratings AS r
    INNER JOIN genres AS g
        ON r.movie_id = g.movie_id

),

aggregated AS (

    SELECT 
        user_id,
        genre, 

        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating

    FROM joined 
    GROUP BY user_Id, genre

)

SELECT * FROM aggregated