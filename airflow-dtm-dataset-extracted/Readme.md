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
The sample contains 195 000 rows without OBJECT_ID and SHAPE
columns. I first chose 225 000 rows which were just
under 100MB. However, GitHub (currently) considers 50MB 
the file size limit, although it allows pushing files
up to 100MB. So, I dropped the number of rows to
195 000 rows and dropped the SHAPE and OBJECT_ID columns.
This brought down the csv file size just below 50 MB.


# References
1. https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires
