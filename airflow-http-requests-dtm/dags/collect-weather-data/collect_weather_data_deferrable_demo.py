import os

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from dags.helpers.shared import read_cities
from dags.helpers.xcom_cleanup import cleanup_xcom_dag
from dags.httpasync.httpdeferrable import HttpSensorAsync
from dags.httpasync.httpdeferrable_not_working import (
    HttpSensorAsync as HttpSensorAsyncNotWorking,
)

DAG_ID = "collect_weather_data_deferrable_demo"

WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    description="This DAG demonstrates the difference in HttpSensorAsync behaviour when mapping over it directly vs mapping over a task group containing the sensor",
    on_success_callback=cleanup_xcom_dag,
    on_failure_callback=cleanup_xcom_dag,
    render_template_as_native_obj=True,
    tags=["airflow2.5", "task mapping", "deferrable", "failing"],
):
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_cities,
    )

    def map_read_data_for_deferrable_tg(batch):
        print("Hi from tg map function")
        return {
            "params_data": [
                {"lat": city["lat"], "lon": city["lng"], "appid": WEATHER_API_KEY}
                for city in batch["batch"]
            ],
        }

    def map_read_data_for_deferrable(batch):
        return [
            {"lat": city["lat"], "lon": city["lng"], "appid": WEATHER_API_KEY}
            for city in batch["batch"]
        ]

    def _empty(params_data):
        print(params_data)
        print(type(params_data))
        return params_data

    @task_group(group_id="http_handling")
    def my_group(params_data):
        empty = PythonOperator(
            task_id="empty",
            python_callable=_empty,
            op_kwargs={"params_data": params_data},
        )

        exercise_sensor = HttpSensorAsyncNotWorking(
            task_id="make_http_requests_not_working_exercise",
            http_conn_id="OWM_API",
            data=params_data,
            endpoint="data/2.5/weather",
            max_active_tis_per_dag=3,
        )

        empty >> exercise_sensor

        HttpSensorAsync(
            task_id="make_http_requests",
            http_conn_id="OWM_API",
            data=params_data,
            endpoint="data/2.5/weather",
            max_active_tis_per_dag=3,
        )

        HttpSensorAsyncNotWorking(
            task_id="make_http_requests_not_working",
            http_conn_id="OWM_API",
            data=params_data,
            endpoint="data/2.5/weather",
            max_active_tis_per_dag=3,
        )

    HttpSensorAsyncNotWorking.partial(
        task_id="make_http_requests_not_working",
        http_conn_id="OWM_API",
        endpoint="data/2.5/weather",
        max_active_tis_per_dag=3,
    ).expand(data=read_data.output.map(map_read_data_for_deferrable))

    mapped_groups = my_group.expand_kwargs(
        read_data.output.map(map_read_data_for_deferrable_tg)
    )
