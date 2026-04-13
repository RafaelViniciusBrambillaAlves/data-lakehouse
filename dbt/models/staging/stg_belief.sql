WITH source AS(

    SELECT * FROM {{ source('silver', 'belief_cleaned') }}

), 

renamed AS (

    SELECT 
        user_id,
        movie_id,
        is_seen,
        watch_date,
        user_rating,
        predicted_rating,
        user_certainty,
        system_rating,
        event_timestamp,
        movie_idx,
        source_type,
        ingestion_timestamp,
        source_system,
        processed_timestamp

    FROM source 

    WHERE user_id IS NOT NULL
      AND movie_id IS NOT NULL

)

SELECT * FROM renamed

