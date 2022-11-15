import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from CustomTimetable import CustomTimetable

with DAG(
    dag_id="custom_timetable_dag",
    description="This DAG demonstrates the use of custom timetables",
    # left the first start_date if you want to play/debug
    # start_date=pendulum.now().add(days=2).subtract(
    #     hours=int(os.environ["HOURS_AGO_FOR_TIMETABLES"])
    # ),
    start_date=pendulum.now().subtract(
        hours=int(os.environ["HOURS_AGO_FOR_TIMETABLES"])
    ),
    timetable=CustomTimetable(),
    # catchup=False, # left for playing
    tags=["airflow2.2", "custom timetable"],
):
    op = BashOperator(task_id="print-time", bash_command="echo {{ ts }}")
