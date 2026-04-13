WITH source AS (

    SELECT * FROM {{ source('silver', 'ratings_cleaned') }}

),

renamed AS (

    SELECT
        user_id,
        movie_id, 
        rating,

        event_timestamp,
        CAST(event_timestamp AS DATE) AS event_date,

        ingestion_timestamp,
        processed_timestamp,

        source_system,
        source_topic
    
    FROM source

    WHERE user_id IS NOT NULL
      AND movie_id IS NOT NULL
      AND rating IS NOT NULL
)

SELECT * FROM renamed