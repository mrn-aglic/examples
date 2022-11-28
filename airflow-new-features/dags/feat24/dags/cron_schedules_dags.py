import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger import CronTriggerTimetable

MINUTES_AGO = 15


def _print_data_interval(**context):
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    logical_date = context["logical_date"]

    logging.info("Data interval start: %s", data_interval_start.to_datetime_string())
    logging.info("Data interval end: %s", data_interval_end.to_datetime_string())
    logging.info("Logical date: %s", logical_date.to_datetime_string())


with DAG(
    dag_id="basic_cron_schedule",
    start_date=pendulum.now().subtract(minutes=MINUTES_AGO),
    schedule="*/5 * * * *",
    description="A simple DAG to demonstrate cron schedule",
    tags=["airflow2.4", "cron"],
):
    cron_op = PythonOperator(
        task_id="print_data_interval", python_callable=_print_data_interval
    )


with DAG(
    dag_id="basic_CronDataIntervalTimetable",
    start_date=pendulum.now().subtract(minutes=MINUTES_AGO),
    schedule=CronDataIntervalTimetable("*/5 * * * *", timezone="UTC"),
    description="A simple DAG to demonstrate CronDataIntervalTimetable",
    tags=["airflow2.4", "cron"],
):
    data_interval_timetable_op = PythonOperator(
        task_id="print_data_interval", python_callable=_print_data_interval
    )

with DAG(
    dag_id="basic_CronTriggerTimetable",
    start_date=pendulum.now().subtract(minutes=MINUTES_AGO),
    schedule=CronTriggerTimetable("*/5 * * * *", timezone="UTC"),
    description="A simple DAG to demonstrate CronTriggerTimetable",
    tags=["airflow2.4", "cron"],
):
    trigger_timetable_op = PythonOperator(
        task_id="print_data_interval", python_callable=_print_data_interval
    )

with DAG(
    dag_id="basic_CronTriggerTimetable_interval",
    start_date=pendulum.now().subtract(minutes=MINUTES_AGO),
    schedule=CronTriggerTimetable(
        "*/5 * * * *", timezone="UTC", interval=timedelta(minutes=3)
    ),
    description="A simple DAG to demonstrate CronTriggerTimetable with inteval",
    tags=["airflow2.4", "cron"],
):
    trigger_timetable_with_interval_op = PythonOperator(
        task_id="print_data_interval", python_callable=_print_data_interval
    )
