import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from feat22.mypkg.httpdeferrable import HttpSensorAsync

with DAG(
    dag_id="defferable_operator_dag",
    description="This DAG demonstrates the use of defferable operators",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    tags=["airflow2.2"],
):
    # let's just check that the package is installed on the scheduler
    pip_show = BashOperator(task_id="pip_show", bash_command="pip show feat22")

    get_data = HttpSensorAsync(
        task_id="get_data",
        http_conn_id="wildfires_api",
        endpoint="api/get_with_delay",
        method="GET",
        data={"start": 0, "limit": 10, "delay": 15},
        retry_limit=3,
        retry_delay=1,
    )
