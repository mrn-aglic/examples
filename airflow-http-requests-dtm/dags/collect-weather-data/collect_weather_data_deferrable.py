import os

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from dags.helpers.shared import (
    cleanup_files,
    collect_and_upload_s3,
    read_cities,
    store_to_temp_file,
)
from dags.helpers.xcom_cleanup import cleanup_xcom_dag, cleanup_xcom_of_previous_tasks
from dags.httpasync.httpdeferrable import HttpSensorAsync

DAG_ID = "collect_weather_data_deferrable"

TMP_STORAGE_LOCATION = f"/tmp/{DAG_ID}"

WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates collecting weather data for multiple cities using dynamic task mapping and deferrable operators",
    on_success_callback=cleanup_xcom_dag,
    tags=["airflow2.5", "task mapping", "deferrable"],
):
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_cities,
    )

    def map_read_data_for_deferrable(city):
        return {**city, "appid": WEATHER_API_KEY}

    @task_group(group_id="http_handling")
    def my_group(city_data):
        make_http_request = HttpSensorAsync(
            task_id="make_http_request",
            http_conn_id="OWM_API",
            data=city_data,
            endpoint="data/2.5/weather",
            max_active_tis_per_dag=3,
        )

        store_to_temp = PythonOperator(
            task_id="store_to_temp",
            python_callable=store_to_temp_file,
            op_kwargs={
                "city_data": city_data,
                "http_response": make_http_request.output,
            },
            on_success_callback=cleanup_xcom_of_previous_tasks,
        )

        make_http_request >> store_to_temp

    collect_data = PythonOperator(
        task_id="collect_data",
        op_kwargs={
            "s3_conn_id": "locals3",
        },
        python_callable=collect_and_upload_s3,
    )

    cleanup_tmp_files = PythonOperator(
        task_id="cleanup_tmp_files", python_callable=cleanup_files
    )

    mapped_groups = my_group.expand(city_data=read_data.output)
    mapped_groups >> collect_data >> cleanup_tmp_files
