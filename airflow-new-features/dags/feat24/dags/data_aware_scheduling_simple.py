import os

import pendulum
from airflow import DAG
from airflow.datasets import Dataset

wildfires_dataset = Dataset("s3://")

with DAG(
    dag_id="data_aware_producer_simple",
    description="This dag demonstrates a simple dataset producer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * * *",
    tags=["airflow2.4", "dataset", "dataset-producer"],
):
    pass


with DAG(
    dag_id="data_aware_consumer_simple",
    description="This dag demonstrates a simple dataset consumer",
    schedule=[wildfires_dataset],
    tags=["airflow2.4", "dataset", "dataset-consumer"],
):
    pass
