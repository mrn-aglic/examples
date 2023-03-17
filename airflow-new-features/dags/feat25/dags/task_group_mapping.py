import json
import logging
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from airflow import DAG
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
    "STAT_CAUSE_CODE",
    "STAT_CAUSE_DESCR",
]

GET_ENDPOINT = "/api/get_for_columns"

USE_SMALL = os.environ["USE_SMALL_API"] == "true"
BATCH_SIZE = (
    int(os.environ["BATCH_SIZE_SMALL"]) if USE_SMALL else int(os.environ["BATCH_SIZE"])
)
COUNT_ENDPOINT = os.environ["COUNT_ENDPOINT"]

if USE_SMALL:
    COUNT_ENDPOINT = f"{COUNT_ENDPOINT}_small"

BUCKET = "datasets"


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
        minio_client.fget_object(
            bucket_name=BUCKET,
            object_name=f"{timestamp}/{map_index}/data_from_{start}.csv",
            file_path=filename,
        )
        df = pd.read_csv(filename)[["FIRE_YEAR"]]
    finally:
        if Path(filename).exists():
            os.remove(filename)

    result = df.groupby("FIRE_YEAR").value_counts()

    logging.info("Number of fires per year:")
    logging.info(result)
    logging.info(result.to_json())

    return result.to_json()


def _final_count(elements):
    result = {}
    total_count = 0

    for el in elements:
        el = json.loads(el)
        for year in el:
            result[year] = el[year] + (result[year] if year in result else 0)
            total_count = total_count + el[year]

    logging.info("The final count is: %s", total_count)
    logging.info("Final count per year:")
    logging.info(pd.DataFrame(result, index=["fire count"]).transpose())


with DAG(
    dag_id="task_group_mapping_example",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
    description="This DAG demonstrates the use of task groups with dynamic task mapping",
    tags=["airflow2.5", "task_group_mapping"],
):
    get_rows_count = BashOperator(
        task_id="get_rows_count",
        bash_command=f"curl http://wildfires-api:8000/api/{COUNT_ENDPOINT}",
        do_xcom_push=True,
    )

    create_batches = PythonOperator(
        task_id="create_batches",
        python_callable=_create_batches,
        op_kwargs={"count": "{{ ti.xcom_pull(task_ids='get_rows_count') }}"},
    )

    @task_group(group_id="batch_processing")
    def processing_group(my_batch):
        conn_id = "wildfires_api"
        s3_conn_id = "locals3"
        endpoint = GET_ENDPOINT

        @task
        def transfer_to_s3(single_batch):
            _transfer_to_s3(
                conn_id=conn_id,
                s3_conn_id=s3_conn_id,
                endpoint=endpoint,
                batch=single_batch,
            )

        @task
        def count_fire_per_year():
            return _count_per_year(s3_conn_id=s3_conn_id)

        # pylint: disable=expression-not-assigned
        transfer_to_s3(my_batch) >> count_fire_per_year()

    final_count = PythonOperator(
        task_id="final_count",
        python_callable=_final_count,
        op_kwargs={
            "elements": "{{ ti.xcom_pull(task_ids='batch_processing.count_fire_per_year') }}"
        },
    )

    get_rows_count >> create_batches
    pg = processing_group.expand(my_batch=create_batches.output)

    pg >> final_count
