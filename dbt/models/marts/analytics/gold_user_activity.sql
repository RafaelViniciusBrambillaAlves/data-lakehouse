{{ config(
    materialized = 'table'
) }}

WITH user_metrics AS(

    SELECT * FROM {{ ref('int_user_metrics') }}

)

SELECT * 
FROM user_metrics 
ORDER BY total_ratings DESC