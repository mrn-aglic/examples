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

wildfires_dataset = Dataset("s3://locals3/datasets/test.csv")


def _transfer_from_api_to_s3(s3_conn_id, api_conn, endpoint, args, bucket, key):

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

        logging.info(result.last_modified)


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
    dag_id="data_aware_producer_simple",
    description="This dag demonstrates a simple dataset producer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    tags=["dataset-producer", "simple"],
):
    transfer_from_api_to_s3 = PythonOperator(
        task_id="transfer_api_to_s3",
        python_callable=_transfer_from_api_to_s3,
        op_kwargs={
            "s3_conn_id": "locals3",
            "api_conn": "wildfires_api",
            "args": {"limit": 1000},
            "bucket": "datasets",
            "endpoint": "api/sample",
            "key": "test.csv",
        },
        outlets=[wildfires_dataset],
    )


with DAG(
    dag_id="data_aware_consumer_simple",
    description="This dag demonstrates a simple dataset consumer",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule=[wildfires_dataset],
    tags=["dataset-consumer", "simple"],
):
    list_objects = PythonOperator(
        task_id="list_objects",
        python_callable=_list_objects,
        op_kwargs={
            "s3_conn_id": "locals3",
            "bucket": "datasets",
        },
    )
