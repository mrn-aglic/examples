import os

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from dags.helpers.shared import (
    cleanup_files,
    collect_and_upload_s3,
    map_response,
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
    render_template_as_native_obj=True,
    tags=["airflow2.5", "task mapping", "deferrable"],
):
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_cities,
    )

    def map_read_data_for_deferrable(batch):
        return {
            "params_data": [
                {"lat": city["lat"], "lon": city["lng"], "appid": WEATHER_API_KEY}
                for city in batch["batch"]
            ],
            "cities_data": batch["batch"],
        }

    def return_data_func(city):
        return {
            "city": city["city_ascii"],
            "country": city["country"],
            "iso3": city["iso3"],
        }

    def join_data_for_storing(result_data, city_data):
        return map_response(result_data, return_data_func(city_data))

    @task_group(group_id="http_handling")
    def my_group(cities_data, params_data):
        make_http_requests = HttpSensorAsync(
            task_id="make_http_requests",
            http_conn_id="OWM_API",
            data=params_data,
            endpoint="data/2.5/weather",
            max_active_tis_per_dag=3,
        )

        store_to_temp = PythonOperator(
            task_id="store_to_temp",
            python_callable=store_to_temp_file,
            op_kwargs={
                "tmp_storage_location": TMP_STORAGE_LOCATION,
                "cities_data": make_http_requests.output.zip(cities_data).map(
                    lambda results: join_data_for_storing(*results)
                ),
            },
            on_success_callback=cleanup_xcom_of_previous_tasks,
        )

        make_http_requests >> store_to_temp

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
        python_callable=cleanup_files,
        op_kwargs={"tmp_storage_location": TMP_STORAGE_LOCATION},
    )

    mapped_groups = my_group.expand_kwargs(
        # cities_data=read_data.output
        read_data.output.map(map_read_data_for_deferrable)
    )

    mapped_groups >> collect_data >> cleanup_tmp_files
