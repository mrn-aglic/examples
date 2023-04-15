import asyncio
import os

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpAsyncHook
from dags.helpers.shared import (
    cleanup_files,
    collect_and_upload_s3,
    map_response,
    read_cities,
    store_to_temp_file,
)
from dags.helpers.xcom_cleanup import cleanup_xcom_dag, cleanup_xcom_of_previous_tasks

DAG_ID = "collect_weather_data"

TMP_STORAGE_LOCATION = f"/tmp/{DAG_ID}"

WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]


async def run_in_loop(async_hook, endpoint, params, return_data):
    response = await async_hook.run(endpoint=endpoint, data=params)
    json_response = await response.json()

    return map_response(json_response, return_data)


def _make_api_request_for_batch(batch, **context):
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

    return data


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates collecting weather data for multiple cities using dynamic task mapping",
    on_success_callback=cleanup_xcom_dag,
    render_template_as_native_obj=True,
    tags=["airflow2.5", "task mapping", "asyncio"],
):
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_cities,
    )

    @task_group(group_id="http_handling")
    def my_group(cities_data):
        make_batch_http_requests = PythonOperator(
            task_id="make_batch_http_requests",
            python_callable=_make_api_request_for_batch,
            op_kwargs=cities_data,
            max_active_tis_per_dag=3,
        )

        store_to_temp = PythonOperator(
            task_id="store_to_temp",
            python_callable=store_to_temp_file,
            op_kwargs={
                "cities_data": make_batch_http_requests.output,
                "tmp_storage_location": TMP_STORAGE_LOCATION,
            },
            on_success_callback=cleanup_xcom_of_previous_tasks,
        )

        make_batch_http_requests >> store_to_temp

    collect_data = PythonOperator(
        task_id="collect_data",
        op_kwargs={
            "s3_conn_id": "locals3",
            "tmp_storage_location": TMP_STORAGE_LOCATION,
        },
        python_callable=collect_and_upload_s3,
    )

    cleanup_tmp_files = PythonOperator(
        task_id="cleanup_tmp_files",
        op_kwargs={"tmp_storage_location": TMP_STORAGE_LOCATION},
        python_callable=cleanup_files,
    )

    mapped_groups = my_group.expand(cities_data=read_data.output)

    mapped_groups >> collect_data >> cleanup_tmp_files
