# Airflow new features

The example here is focused on demonstrating new features
introduced in Apache Airflow 2.2, 2.3 and 2.4.


**One of the most important changes in newer versions of
Airflow is the deprecation of `execution_date`. 
Look at reference [1]**

Here is the list of demonstrated new features in Airflow 2.2:
1. [x] Custom timetables
2. [x] Deferrable operators and triggers

Here is the list of demonstrated new features in Airflow 2.3:
1. [x] Dynamic Task Mapping
2. [x] Tree view replaced by Grid view (demonstrated by default)
3. [x] LocalKubernetesExecutor
4. [ ] Reuse of decorated tasks
5. [ ] Various Minor features:
   1. [ ] Store connections from JSON format

Here is the list of demonstrated new features in Airflow 2.4:
1. [x] Data-aware (data-driven) scheduling
2. [x] CronTriggerTimetable vs CrontDataIntervalTimetable
3. [X] Various minor features:
   1. [x] Consolidated schedule parameter
   2. [x] Auto-register DAG used in context manager (used in all examples)

# Before running the examples
A one time start of `prep.sh` is required to copy the 
`.env.backup`files to `.env`.

You can run it with:
```shell
make prep
```

# Running the examples

You can run all of the examples with
```shell
docker-compose up
```
or
```shell
make run
```

You can also run only examples specific to certain versions
e.g.: 
```shell
make run-2.2
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
1. https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
2. https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires
