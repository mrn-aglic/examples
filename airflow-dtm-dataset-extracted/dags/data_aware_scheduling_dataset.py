# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals

import logging
import os
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from minio import Minio

BATCH_SIZE = 1000

wildfires_dataset = Dataset("s3://locals3/datasets/")


def _transfer_from_api_to_s3(s3_conn_id, api_conn, args, bucket, key, endpoint):

    connection = BaseHook.get_connection(api_conn)

    url = f"{connection.conn_type}://{connection.host}:{connection.port}/{endpoint}"
    query_string = urlencode(args)

    url = f"{url}?{query_string}"

    response = requests.get(url)

    try:
        logging.info("Attempting to get data from api")
        response.raise_for_status()
    except Exception as ex:
        logging.error("Exception occured while trying to get response from API")
        logging.error(ex)

    data = response.json()

    df = pd.DataFrame.from_records(data)

    with NamedTemporaryFile("w+b") as file:
        df.to_csv(file.name, index=False)

        conn = BaseHook.get_connection(conn_id=s3_conn_id)

        minio_client = Minio(
            conn.extra_dejson["host"].split("://")[1],
            access_key=conn.login,
            secret_key=conn.password,
            secure=False,
        )

        logging.info("Storing object: %s/%s.", bucket, key)

        minio_client.remove_object(bucket_name=bucket, object_name=key)

        result = minio_client.fput_object(
            bucket_name=bucket, object_name=key, file_path=file.name
        )

        logging.info(result)


def _list_objects(s3_conn_id, bucket):
    conn = BaseHook.get_connection(conn_id=s3_conn_id)

    minio_client = Minio(
        conn.extra_dejson["host"].split("://")[1],
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )

    result = minio_client.list_objects(bucket_name=bucket)

    logging.info("Listing objects:")
    for item in result:
        logging.info(item.object_name)


with DAG(
    dag_id="data_aware_producer_dataset",
    description="This dag demonstrates a simple dataset producer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    tags=["dataset-producer", "task-mapping"],
):

    def _get_data_range():
        return [
            {
                "s3_conn_id": "locals3",
                "api_conn": "wildfires_api",
                "bucket": "datasets",
                "key": f"test_{i}.csv",
                "args": {"start": i * BATCH_SIZE, "limit": BATCH_SIZE},
                "endpoint": "api/get",
            }
            for i in range(5)
        ]

    transfer_from_api_to_s3 = PythonOperator.partial(
        task_id="transfer_api_to_s3",
        python_callable=_transfer_from_api_to_s3,
        outlets=[wildfires_dataset],
    ).expand(op_kwargs=_get_data_range())


with DAG(
    dag_id="data_aware_consumer_dataset",
    description="This dag demonstrates a simple dataset consumer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule=[wildfires_dataset],
    tags=["dataset-consumer", "task-mapping"],
):
    list_objects = PythonOperator(
        task_id="list_objects",
        python_callable=_list_objects,
        op_kwargs={
            "s3_conn_id": "locals3",
            "bucket": "datasets",
        },
    )
