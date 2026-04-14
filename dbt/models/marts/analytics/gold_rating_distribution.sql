{{ config(
    materialized = 'table'
) }}

WITH ratings AS (

    SELECT * FROM {{ ref('stg_ratings') }}

), 

distribution  AS (

    SELECT 
        rating, 
        COUNT(*) AS total_ratings
    
    FROM ratings
    GROUP BY rating

)

SELECT * FROM distribution ORDER BY rating