{{ config(
    materialized = 'table'
) }}

WITH movie_metrics AS(

    SELECT * FROM {{ ref('int_movie_metrics') }}

),

final AS (

    SELECT 
        movie_id, 
        title,
        release_year,

        total_ratings AS popularity,
        avg_rating AS quality
    
    FROM movie_metrics

)

SELECT * FROM final



