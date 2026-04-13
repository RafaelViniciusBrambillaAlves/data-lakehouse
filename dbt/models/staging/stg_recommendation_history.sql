WITH source AS (

    SELECT * FROM {{ source('silver', 'recommendation_history_cleaned') }}

),

renamed AS (

    SELECT
        user_id, 
        movie_id,
        predicted_rating,
        event_timestamp,
        ingestion_timestamp,
        source_system,
        source_topic,
        processed_timestamp

    FROM source

    WHERE user_id IS NOT NULL
      AND movie_id IS NOT NULL
)

SELECT * FROM renamed