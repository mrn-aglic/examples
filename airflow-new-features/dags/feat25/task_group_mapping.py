import logging
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from airflow import DAG, XComArg
from airflow.decorators import task, task_group
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.http.hooks.http import HttpHook
from minio import Minio

COLUMNS_OF_INTEREST = [
    "FOD_ID",
    "LATITUDE",
    "LONGITUDE",
    "COUNTY",
    "FIRE_YEAR",
    "FIRE_DOY",
    "STAT_CAUSE_CODE",
    "STAT_CAUSE_DESCR",
]

COLUMNS_ENDPOINT = "/api/get_columns"
GET_ENDPOINT = "/api/get_for_columns"

BATCH_SIZE = int(os.environ["BATCH_SIZE"])
COUNT_ENDPOINT = os.environ["COUNT_ENDPOINT"]

BUCKET = "task-group-mapping"


def _create_batches(count):
    count = int(count)

    num_batches = math.ceil(count / BATCH_SIZE)
    return [{"start": i * BATCH_SIZE} for i in range(num_batches)]


# pylint:disable=too-many-locals
def _transfer_to_s3(conn_id, s3_conn_id, endpoint, batch):
    context = get_current_context()
    task_instance = context["ti"]
    map_index = task_instance.map_index

    start = batch["start"]

    timestamp = context["task"].render_template("{{ ts_nodash }}", context)

    api_conn = BaseHook.get_connection(conn_id)

    url = f"{api_conn.conn_type}://{api_conn.host}:{api_conn.port}"

    logging.info("Sending HTTP GET request: %s", url)

    http_hook = HttpHook(http_conn_id=conn_id, method="GET")
    response = http_hook.run(
        endpoint,
        data={"start": start, "limit": BATCH_SIZE, "columns": COLUMNS_OF_INTEREST},
    )

    logging.info("Got response from API server")
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

        key = f"{timestamp}/{map_index}/data_from_{start}.csv"
        logging.info("Storing object: %s/%s.", BUCKET, key)

        minio_client.remove_object(bucket_name=BUCKET, object_name=key)

        result = minio_client.fput_object(
            bucket_name=BUCKET, object_name=key, file_path=file.name
        )

        logging.info("Result of uploading file:")
        logging.info(result)


def _count_per_year(s3_conn_id):
    context = get_current_context()
    task_instance = context["ti"]
    map_index = task_instance.map_index

    timestamp = context["task"].render_template("{{ ts_nodash }}", context)

    conn = BaseHook.get_connection(conn_id=s3_conn_id)

    minio_client = Minio(
        conn.extra_dejson["host"].split("://")[1],
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )

    start = map_index * BATCH_SIZE
    filename = f"temp-{map_index}-{timestamp}.csv"

    if Path(filename).exists():
        os.remove(filename)

    try:
        response = minio_client.fget_object(
            bucket_name=BUCKET,
            object_name=f"{timestamp}/{map_index}/data_from_{start}.csv",
            file_path=filename,
        )
        df = pd.read_csv(filename)
    finally:
        response.close()
        response.release_conn()
        os.remove(filename)

    result = df.groupby("FIRE_YEAR").count()

    logging.info("Number of fires per year:")
    logging.info(result)

    return result.to_json()


def _final_count():
    pass


with DAG(
    dag_id="task_group_mapping_example__________",
    start_date=pendulum.now().subtract(
        hours=1
    ),  # (hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates the use of task groups with dynamic task mapping",
    tags=["airflow2.5", "task_group_mapping"],
):
    # get_rows_count = BashOperator(
    #     task_id="get_rows_count",
    #     bash_command=f"curl http://wildfires-api:8000/api/{COUNT_ENDPOINT}",
    #     do_xcom_push=True,
    # )
    #
    # create_batches = PythonOperator(
    #     task_id="create_batches",
    #     python_callable=_create_batches,
    #     op_kwargs={"count": "{{ ti.xcom_pull(task_ids='get_rows_count') }}"},
    # )
    # # @task
    # # def create_batches(rows_count):
    # #     return _create_batches(rows_count)
    #
    #
    # @task_group(group_id="batch_processing")
    # def processing_group(my_batch):
    #     conn_id = "wildfires_api"
    #     s3_conn_id = "locals3"
    #     endpoint = GET_ENDPOINT
    #
    #     @task
    #     def transfer_to_s3(single_batch):
    #         _transfer_to_s3(
    #             conn_id=conn_id,
    #             s3_conn_id=s3_conn_id,
    #             endpoint=endpoint,
    #             batch=single_batch,
    #         )
    #
    #     @task
    #     def count_fire_per_year():
    #         _count_per_year(s3_conn_id=s3_conn_id)
    #
    #
    #     transfer_to_s3(my_batch) >> count_fire_per_year()
    #
    #
    # final_count = PythonOperator(
    #     task_id="final_count",
    #     python_callable=_final_count,
    #
    # )
    #
    #
    # get_rows_count >> create_batches
    # pg = processing_group.partial().expand(my_batch=create_batches.output)
    #
    # pg >> final_count

    # creating a task group using the decorator with the dynamic input my_num
    @task_group(group_id="group1")
    def tg1(my_num):
        @task
        def print_num(num):
            return num

        @task
        def add_42(num):
            s = num["start"]
            return s + 42

        add_42(print_num(my_num))

    @task
    def cde():
        return [{"start": 1}, {"start": 2}]

    res = cde()
    tg1_object = tg1.expand(my_num=res)

    # def c():
    #     return [1, 2, 3]
    #
    # cd = PythonOperator(
    #     task_id="cd",
    #     python_callable=c
    # )

    # res >> tg1
    # cd >> tg1_object
