WITH source AS(

    SELECT * FROM {{ source('silver', 'elicitation_cleaned') }}

),

renamed AS(

    SELECT 
        movie_id,
        month_idx,
        source_type,
        event_timestamp,
        ingestion_timestamp,
        source_system,
        processed_timestamp

    FROM source 

    WHERE movie_id IS NOT NULL

)

SELECT * FROM renamed