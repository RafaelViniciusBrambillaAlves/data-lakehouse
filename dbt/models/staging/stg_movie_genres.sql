WITH source AS (

    SELECT * FROM {{ source('silver', 'movie_genres') }}

),

cleaned AS (

    SELECT
        movie_id,
        genre

    FROM source

    WHERE movie_id IS NOT NULL
      AND genre IS NOT NULL
      AND genre != '(no genres listed)'

)

SELECT * FROM cleaned