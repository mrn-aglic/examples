import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="custom_timetable_dag",
    description="This DAG demonstrates the use of custom timetables",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
):
    op = BashOperator(task_id="print-time", bash_command="echo {{ ts }}")
