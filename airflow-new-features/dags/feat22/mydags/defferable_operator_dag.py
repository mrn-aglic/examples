import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from feat22.mypkg.httpdeferrable import HttpSensorAsync


def check(**context):
    # pylint: disable=import-outside-toplevel
    import feat22

    help(feat22)


with DAG(
    dag_id="defferable_operator_dag",
    description="This DAG demonstrates the use of defferable operators",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    tags=["airflow2.2"],
):
    # let's just check that the package is accessible from source on the scheduler
    python_check_module_accessible = BashOperator(
        task_id="python_check_module_accessible",
        bash_command='python -c "import feat22; help(feat22)"',
    )

    python_check_module_accessible_from_source = PythonOperator(
        task_id="python_check_module_accessible_from_source",
        python_callable=check,
    )

    get_data = HttpSensorAsync(
        task_id="get_data",
        http_conn_id="wildfires_api",
        endpoint="api/get_with_delay",
        method="GET",
        data={"start": 0, "limit": 10, "delay": 15},
        retry_limit=3,
        retry_delay=1,
    )
