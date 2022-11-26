import logging
import math
import os
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from minio import Minio

BATCH_SIZE = int(os.environ["BATCH_SIZE"])
ENDPOINT = os.environ["ENDPOINT"]
COUNT_ENDPOINT = os.environ["COUNT_ENDPOINT"]
BUCKET = "api-data"
EXAMPLE = "simple"


def _create_batches(count):
    count = int(count)

    num_batches = math.ceil(count / BATCH_SIZE)
    return [{"start": i * BATCH_SIZE} for i in range(num_batches)]


# pylint:disable=too-many-locals
def _transfer_to_s3(conn_id, s3_conn_id, endpoint, start, **context):
    timestamp = context["task"].render_template("{{ ts_nodash }}", context)

    api_conn = BaseHook.get_connection(conn_id)

    url = f"{api_conn.conn_type}://{api_conn.host}:{api_conn.port}"

    logging.info("Sending HTTP GET request: %s", url)

    http_hook = HttpHook(http_conn_id=conn_id, method="GET")
    response = http_hook.run(endpoint, data={"start": start, "limit": BATCH_SIZE})

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

        key = f"{EXAMPLE}/{timestamp}/data_from_{start}.csv"
        logging.info("Storing object: %s/%s.", BUCKET, key)

        minio_client.remove_object(bucket_name=BUCKET, object_name=key)

        result = minio_client.fput_object(
            bucket_name=BUCKET, object_name=key, file_path=file.name
        )

        logging.info("Result of uploading file:")
        logging.info(result)


with DAG(
    dag_id="dynamic_task_mapping_example_simple",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates the use of Dynamic task mapping",
    tags=["airflow2.3", "task mapping"],
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

    def prep_arg(batch):
        return {
            "conn_id": "wildfires_api",
            "s3_conn_id": "locals3",
            "endpoint": ENDPOINT,
            **batch,
        }

    transfer_to_s3 = PythonOperator.partial(
        task_id="transfer_to_s3",
        python_callable=_transfer_to_s3,
        max_active_tis_per_dag=4,
    ).expand(op_kwargs=create_batches.output.map(prep_arg))

    get_rows_count >> create_batches
