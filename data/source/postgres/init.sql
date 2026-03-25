CREATE SCHEMA IF NOT EXISTS raw;

-- movies
CREATE TABLE raw.movies (
    movie_id INT PRIMARY KEY,
    title TEXT,
    genres TEXT
);

-- movie elicitation
CREATE TABLE raw.movie_elicitation_set (
    movie_id INT,
    month_idx INT,
    source INT,
    tstamp TIMESTAMP
);

-- belief data
CREATE TABLE raw.belief_data (
    user_id INT,
    movie_id INT,
    is_seen SMALLINT,
    watch_date TEXT,
    user_elicit_rating FLOAT,
    user_predict_rating FLOAT,
    user_certainty FLOAT,
    tstamp TIMESTAMP,
    movie_idx INT,
    source INT,
    system_predict_rating FLOAT
);

-- índices
CREATE INDEX idx_belief_user ON raw.belief_data(user_id);
CREATE INDEX idx_belief_movie ON raw.belief_data(movie_id);