WITH source AS (

    SELECT * FROM {{ source('silver', 'belief_features') }}

),

renamed AS (

    SELECT 
        user_id,
        movie_id,
        is_seen,
        has_watched,
        user_rating,
        system_rating,
        rating_diff,
        user_certainty,
        is_high_confidence,
        event_timestamp,
        event_date
    
    FROM source

    WHERE user_id IS NOT NULL
      AND movie_id IS NOT NULL

)

SELECT * FROM renamed