import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue

SENSOR_PASS_STATUS_CODES = "200-208"

APPEND_STATUS_CODES = ",".join([SENSOR_PASS_STATUS_CODES for i in range(5)])

STATUS_CODES = (
    f"100-103,226,300-308,400-426,428-431,451,500-508,510,511,{APPEND_STATUS_CODES}"
)

API_ENDPOINT = "https://httpstat.us/random"

HEADERS = {"Accept": "application/json"}

with DAG(
    dag_id="sensor_decorator",
    start_date=pendulum.now().subtract(hours=2),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
    description="This DAG demonstrates how to use the @task.sensor decorator",
    tags=["airflow2.5", "sensor", "decorator"],
):

    @task.sensor(poke_interval=10, timeout=600, mode="reschedule")
    def ping_api():
        response = requests.get(f"{API_ENDPOINT}/{STATUS_CODES}", headers=HEADERS)

        return_value = None
        print(f"Status code: {response.status_code}")

        if response.status_code in range(200, 209):
            condition_met = True
            return_value = response.json()
        else:
            condition_met = False
            print(f"Condition not met, received status code: {response.status_code}")

        now = pendulum.now().timestamp()

        return PokeReturnValue(
            is_done=condition_met,
            xcom_value={"json_response": return_value, "timestamp": now},
        )

    def log_result(result):
        json_response = result["json_response"]
        timestamp = result["timestamp"]

        status_code = json_response["code"]
        description = json_response["description"]

        print(
            f"Obtained status_code: {status_code} at timestamp {pendulum.from_timestamp(timestamp)} "
            f"with description: {description}"
        )

    log = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
        op_kwargs={"result": "{{ ti.xcom_pull(task_ids='ping_api') }}"},
    )

    # pylint: disable=expression-not-assigned
    ping_api() >> log
