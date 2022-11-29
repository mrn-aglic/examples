# An example of a data race in Airflow

These examples have been created to accompany a 
series of 3 stories I published on Medium related
to an issue we faced in our production environment:
1. part 1: https://medium.com/@MarinAgli1/a-subtle-data-race-with-airflow-part-1-ffc26ae82f24
2. part 2: https://medium.com/@MarinAgli1/a-subtle-data-race-with-airflow-part-2-a-quick-fix-363afd51e16e
3. part 3: https://medium.com/@MarinAgli1/a-subtle-data-race-with-airflow-part-3-3-latestonlyoperator-71b1c0aa7309

## Postgres-source
While building the docker image for postgres-source,
it is populated by the movies.csv data.
The image mysql-sink is populated with the 
ratings_smaller.csv data. Both csvs are located in
the dataset folder. 

The dataset was taken from Kaggle (see details
in folder).
