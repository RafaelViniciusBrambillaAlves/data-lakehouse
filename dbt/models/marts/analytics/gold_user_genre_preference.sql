{{ config(
    materialized = 'table'
) }}

WITH base AS (

    SELECT * FROM {{ ref('int_user_genre_metrics') }}

),

final AS (

    SELECT 
        user_id,
        genre, 

        total_ratings,
        avg_rating,

        ROW_NUMBER() OVER(
            PARTITION BY user_id
            ORDER BY avg_rating DESC, total_ratings DESC
        ) AS rank_genre

    FROM base

)

SELECT * FROM final