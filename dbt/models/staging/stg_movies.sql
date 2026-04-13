WITH source AS (

    SELECT * FROM {{ source('silver', 'movies_cleaned') }}

),

renamed AS (

    SELECT
        movie_id,
        title,
        release_year,
        genres_raw,
        ingestion_timestamp,
        processed_timestamp

    FROM source

    -- Drop rows without a valid identifier (data quality guard)
    WHERE movie_id IS NOT NULL
      AND title IS NOT NULL

)

SELECT * FROM renamed