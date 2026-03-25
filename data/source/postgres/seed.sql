-- movies
COPY raw.movies(
    movie_id,
    title,
    genres
)
FROM '/data/csv/movies.csv'
DELIMITER ','
CSV HEADER;

-- movie_elicitation_set
COPY raw.movie_elicitation_set(
    movie_id,
    month_idx,
    source,
    tstamp
)
FROM '/data/csv/movie_elicitation_set.csv'
DELIMITER ','
CSV HEADER;

-- belief_data
COPY raw.belief_data(
    user_id,
    movie_id,
    is_seen,
    watch_date,
    user_elicit_rating,
    user_predict_rating,
    user_certainty,
    tstamp,
    movie_idx,
    source,
    system_predict_rating
)
FROM '/data/csv/belief_data.csv'
DELIMITER ','
CSV HEADER;