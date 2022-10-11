CREATE DATABASE bq;

USE bq;

CREATE TABLE IF NOT EXISTS movies (
  imdbId NCHAR(9),
  movieId INTEGER,
  title VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS ratings(
    movieId INTEGER,
    rating FLOAT,
    timestamp TIMESTAMP
);

LOAD DATA INFILE '/data/ratings_smaller.csv'
INTO TABLE bq.ratings
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movieId, rating, @timestamp)
SET timestamp = FROM_UNIXTIME(@timestamp);

CREATE TABLE IF NOT EXISTS movie_ratings (
  id INT,
  imdb_id NCHAR(9),
  movie_title VARCHAR(200),
  avg_rating FLOAT,
  num_ratings INTEGER,
  logical_date TIMESTAMP
);


USE mysql;
CREATE USER 'myuser' IDENTIFIED BY 'moviespass';

SELECT * FROM mysql.user where USER = 'myuser';

GRANT ALL PRIVILEGES ON bq.* TO myuser;

USE bq;

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO myuser;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO myuser;
