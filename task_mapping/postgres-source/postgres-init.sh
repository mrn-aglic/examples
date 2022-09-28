#!/bin/bash

SEASON="2021-2022"

echo "THE SEASON IS $SEASON"

set -eux

# Create user, database and permissions
#psql -v ON_ERROR_STOP=1 <<-EOSQL
#  CREATE USER taxi WITH PASSWORD 'ridetlc';
#  CREATE DATABASE tlctriprecords;
#  GRANT ALL PRIVILEGES ON DATABASE tlctriprecords TO taxi;
#  \c tlctriprecords;
#  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO taxi;
#  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO taxi;
#EOSQL
psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE DATABASE soccer;
EOSQL

# Create table
psql -v ON_ERROR_STOP=1 soccer <<-EOSQL
  CREATE TABLE IF NOT EXISTS match_scores (
    date TIMESTAMP,
    HomeTeam VARCHAR(20),
    AwayTeam VARCHAR(20),
    FTHG INTEGER,
    FTAG INTEGER,
    FTR NCHAR(1),
    HTHG INTEGER,
    HTAG INTEGER,
    HTR NCHAR(1),
    Referee VARCHAR(30),
    HS INTEGER,
    "AS" INTEGER,
    HST INTEGER,
    AST INTEGER,
    HF INTEGER,
    AF INTEGER,
    HC INTEGER,
    AC INTEGER,
    HY INTEGER,
    AY INTEGER,
    HR INTEGER,
    AR INTEGER
  );
EOSQL

# Load data
files="/data/soccer21-22.csv"

for file in ${files}
do
  echo "Reading $file\n"

  time psql -v ON_ERROR_STOP=1 soccer <<-EOSQL
    set datestyle = euro;

    COPY match_scores(
      Date    ,
      HomeTeam,
      AwayTeam,
      FTHG    ,
      FTAG    ,
      FTR     ,
      HTHG    ,
      HTAG    ,
      HTR     ,
      Referee ,
      HS      ,
      "AS"      ,
      HST     ,
      AST     ,
      HF      ,
      AF      ,
      HC      ,
      AC      ,
      HY      ,
      AY      ,
      HR      ,
      AR
    )
    FROM '$file' DELIMITER ',' CSV HEADER;
EOSQL
done

psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE USER myuser WITH PASSWORD 'matchpass';
  GRANT ALL PRIVILEGES ON DATABASE soccer TO myuser;
  \c soccer;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO myuser;
  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO myuser;
EOSQL

pg_ctl stop

#psql -v ON_ERROR_STOP=1 <<-EOSQL
#  CREATE USER myuser WITH PASSWORD 'matchpass';
#  GRANT ALL PRIVILEGES ON DATABASE soccer TO myuser;
#  \c soccer;
#  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO myuser;
#  GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO myuser;
#EOSQL
