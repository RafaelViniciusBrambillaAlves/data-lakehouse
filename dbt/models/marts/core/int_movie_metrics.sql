{{ config(
    materialized = 'view'
) }}

WITH ratings AS (

    SELECT * FROM {{ ref('stg_ratings') }}

),

movies AS (

    SELECT * FROM {{ ref('stg_movies') }}

),

aggregated AS (

    SELECT 
        movie_id,

        COUNT(*) as total_ratings,
        AVG(rating) AS avg_rating

    FROM ratings
    GROUP BY movie_id
),

joined AS (

    SELECT 
        a.movie_id,
        m.title,
        m.release_year,

        a.total_ratings,
        a.avg_rating

    FROM aggregated AS a
    LEFT JOIN movies AS m
        ON a.movie_id = m.movie_id

),

filtered AS (

    SELECT * 
    FROM joined 
    WHERE total_ratings >= 50

)

SELECT * FROM filtered


