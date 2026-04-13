WITH source AS(

    SELECT * FROM {{ source('silver', 'elicitation_features') }}

), 

renamed AS(

    SELECT 
        movie_id,
        month_idx,
        source_type,
        source_category,
        month_group,
        event_timestamp,
        is_recent,
        event_date

    FROM source 

    WHERE movie_id IS NOT NULL

)

SELECT * FROM renamed 