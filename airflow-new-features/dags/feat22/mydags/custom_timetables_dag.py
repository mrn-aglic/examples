import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from CustomTimetable import CustomTimetable

with DAG(
    dag_id="custom_timetable_dag",
    description="This DAG demonstrates the use of custom timetables",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    timetable=CustomTimetable(),
    tags=["airflow2.2", "custom timetable"],
):
    op = BashOperator(task_id="print-time", bash_command="echo {{ ts }}")
