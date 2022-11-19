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
1. [ ] Dynamic Task Mapping
2. [x] Tree view replaced by Grid view (demonstrated by default)
3. [ ] LocalKubernetesExecutor
4. [ ] Reuse of decorated tasks
5. [ ] Store connections from JSON format

Here is the list of demonstrated new features in Airflow 2.4:
1. [x] Data-aware (data-driven) scheduling
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
with: 
```shell
make run-2.2
```

# Wildfires data
The wildfires data used in the repo is a sample of the 
1.88 million wildfires from Kaggle ([see here](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)).
The sample contains 225 000 rows. I chose this amount because
the csv file ends up with just below 100 MB, which is the GitHub
file size limit for a single file. 

# References
1. https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
2. https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires
