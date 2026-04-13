WITH source as (

    SELECT * FROM {{ source('silver', 'recommendation_features') }}

),

renamed AS (

    SELECT 
        user_id,
        movie_id,
        predicted_rating,
        predicted_rating_category,
        is_high_score,
        predicted_rating_bucket,
        event_timestamp,
        event_date

    FROM source

    WHERE user_id IS NOT NULL
      AND movie_id IS NOT NULL

)

SELECT * FROM renamed