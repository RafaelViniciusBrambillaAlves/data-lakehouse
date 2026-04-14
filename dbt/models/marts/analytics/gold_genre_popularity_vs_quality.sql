{{ config(
    materialized = 'table'
) }}

WITH genre_metrics AS (

    SELECT * FROM {{ ref('int_genre_metrics') }}

), 

final AS (

    SELECT 
        genre, 

        total_ratings AS popularity,
        avg_rating AS quality
    
    FROM genre_metrics

)

SELECT * 
FROM final 
ORDER BY popularity DESC