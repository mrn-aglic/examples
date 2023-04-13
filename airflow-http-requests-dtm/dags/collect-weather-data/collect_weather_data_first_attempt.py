import asyncio
import glob
import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpAsyncHook
from dags.helpers.xcom_cleanup import cleanup_xcom, cleanup_xcom_dag
from minio import Minio

DAG_ID = "collect_weather_data_first_attempt"

TMP_STORAGE_LOCATION = f"/tmp/{DAG_ID}"

DATA_DIR = "/data"
USE_COLS = ["city_ascii", "lat", "lng", "country", "iso2", "iso3"]


WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]

BATCH_SIZE = 5000
BATCH_SIZE = 5


def read_cities():
    filename = f"{DATA_DIR}/worldcities.csv"

    # keep_default_na because of iso2 of Namibia = NA
    world_cities = pd.read_csv(filename, usecols=USE_COLS, keep_default_na=False)
    # keep the number of requests small
    world_cities = world_cities.sample(25)

    data = world_cities.to_dict(orient="records")

    result = [
        {"batch": data[start : start + BATCH_SIZE]}
        for start in range(0, len(data), BATCH_SIZE)
    ]

    return result


def map_response(json_response: dict, return_data: dict):
    return {
        **return_data,
        "weather_descr": json_response["weather"][0]["description"],
        "weather": json_response["weather"][0]["main"],
        "temp": json_response["main"]["temp"],
        "feels_like": json_response["main"]["feels_like"],
        "wind_speed": json_response["wind"]["speed"],
        "lat": json_response["coord"]["lat"],
        "lng": json_response["coord"]["lon"],
        "timestamp": pendulum.now().to_iso8601_string(),
    }


async def run_in_loop(async_hook, endpoint, params, return_data):
    response = await async_hook.run(endpoint=endpoint, data=params)
    json_response = await response.json()

    return map_response(json_response, return_data)


def _make_api_request_for_batch(batch, **context):
    task_instance = context["ti"]
    map_index = task_instance.map_index
    logical_date = context["logical_date"]

    async_hook = HttpAsyncHook(method="GET", http_conn_id="OWM_API")
    loop = asyncio.get_event_loop()

    coroutines = []

    for city in batch:
        endpoint = "data/2.5/weather"

        params = {"appid": WEATHER_API_KEY, "lat": city["lat"], "lon": city["lng"]}

        return_data = {
            "city": city["city_ascii"],
            "country": city["country"],
            "iso3": city["iso3"],
        }

        coroutines.append(run_in_loop(async_hook, endpoint, params, return_data))

    data = loop.run_until_complete(asyncio.gather(*coroutines))

    Path.mkdir(
        Path(f"{TMP_STORAGE_LOCATION}/{logical_date}/"), parents=True, exist_ok=True
    )

    df = pd.DataFrame.from_records(data)
    file_path = f"{TMP_STORAGE_LOCATION}/{logical_date}/{map_index}-tmp.json"
    df.to_json(file_path)


def _collect_data(s3_conn_id, **context):
    logical_date = context["logical_date"]

    all_files = glob.glob(f"{TMP_STORAGE_LOCATION}/{logical_date}/*-tmp.json")

    file_dfs = []

    for file in all_files:
        file_df = pd.read_json(file)
        file_dfs.append(file_df)

    df = pd.concat(file_dfs, ignore_index=True)

    conn = BaseHook.get_connection(conn_id=s3_conn_id)

    minio_client = Minio(
        conn.extra_dejson["host"].split("://")[1],
        access_key=conn.login,
        secret_key=conn.password,
        secure=False,
    )

    with NamedTemporaryFile("w+b") as file:
        df.to_csv(file.name, index=False)

        bucket = "weatherdata"
        key = f"{logical_date}.csv"
        logging.info("Storing object: %s/%s.", bucket, key)

        minio_client.remove_object(bucket_name=bucket, object_name=key)

        result = minio_client.fput_object(
            bucket_name=bucket, object_name=key, file_path=file.name
        )

        logging.info("Result of uploading file:")
        logging.info(result)


def _cleanup_files(**context):
    logical_date = context["logical_date"]
    tmp_storage = Path(f"{TMP_STORAGE_LOCATION}/{logical_date}/")

    all_files = glob.glob(f"{TMP_STORAGE_LOCATION}/{logical_date}/*-tmp.json")

    for file in all_files:
        os.remove(file)

    tmp_storage.rmdir()


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates collecting weather data for multiple cities using dynamic task mapping",
    on_success_callback=cleanup_xcom_dag,
    tags=["airflow2.5", "task mapping"],
):
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_cities,
    )

    make_batch_http_requests = PythonOperator.partial(
        task_id="make_batch_http_requests",
        python_callable=_make_api_request_for_batch,
        on_success_callback=cleanup_xcom,
    ).expand(op_kwargs=read_data.output)

    collect_data = PythonOperator(
        task_id="collect_data",
        op_kwargs={
            "s3_conn_id": "locals3",
        },
        python_callable=_collect_data,
    )

    cleanup_tmp_files = PythonOperator(
        task_id="cleanup_tmp_files", python_callable=_cleanup_files
    )

    make_batch_http_requests >> collect_data >> cleanup_tmp_files

    # NotImplementedError: operator expansion in an expanded task group is not yet supported
    # @task_group(group_id="batch_processing")
    # def processing_group(batch):
    #
    #     @task
    #     def collect_data(ex):
    #         _collect_data(**ex)
    #
    #     collect_data.partial().expand(ex=batch)
    #
    #
    # process_node = processing_group.expand(batch=read_data.output)

    # make_http_request = PythonOperator.partial(
    #     task_id="http_request",
    #     python_callable=_collect_data,
    # ).expand_kwargs(read_data.output)
