# Airflow data-aware scheduling and task mapping

The example here is extracted from the airflow-new-features
directory.


# Before running the examples
A one time start of `prep.sh` is required to copy the 
`.env.backup`files to `.env`.

You can run it with:
```shell
make prep
```

# Running the examples
The examples can be run with
```shell
make run
```
or 
```shell
docker-compose up
```

# Wildfires data
The wildfires data used in the repo is a sample of the 
1.88 million wildfires from Kaggle ([see here](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)).
The sample contains 225 000 rows. I chose this amount because
the csv file ends up with just below 100 MB, which is the GitHub
file size limit for a single file. 

# References
1. https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires
