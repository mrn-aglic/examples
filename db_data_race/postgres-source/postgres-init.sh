#!/bin/bash

SEASON="2021-2022"

echo "LOADING MOVIES DATA INTO POSTGRES"

set -eux

psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE DATABASE movies;
EOSQL

# Create table movies
psql -v ON_ERROR_STOP=1 movies <<-EOSQL
  CREATE TABLE IF NOT EXISTS movies (
    id INTEGER,
    imdbId NCHAR(9),
    title VARCHAR(200)
  );
EOSQL

# Load data
movies_file="/data/movies.csv"

echo "Reading $movies_file\n"

psql -v ON_ERROR_STOP=1 movies <<-EOSQL
    COPY movies(
      imdbId,
      id,
      title
    )
    FROM '$movies_file' DELIMITER ',' CSV HEADER;
EOSQL

psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE USER myuser WITH PASSWORD 'moviespass';
  GRANT ALL PRIVILEGES ON DATABASE movies TO myuser;
  \c movies;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO myuser;
  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO myuser;
EOSQL

pg_ctl stop
