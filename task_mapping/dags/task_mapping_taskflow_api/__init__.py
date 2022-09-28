import logging
import math
import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PYTHON_OPERATOR_SQL = "SELECT COUNT(*) as CNT FROM match_scores"
BATCH_SIZE = 80

SQL_SELECT_BATCH = (
    "SELECT * FROM match_scores OFFSET {{batch_start}} LIMIT {batch_size}".format(
        batch_size=BATCH_SIZE
    )
)

with DAG(
    dag_id="task_mapping_taskflow_api",
    description="DAG demonstrating a data race condition.",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule_interval="0 * * * *",
) as dag:

    @task
    def get_batches(conn_id, sql, **context):
        postgres_hook = PostgresHook(conn_id)
        rows = postgres_hook.get_records(sql)
        count = rows[0][0]

        return [
            {"batch_start": str(i * BATCH_SIZE), "batch_end": str((i + 1) * BATCH_SIZE)}
            for i in range(math.ceil(count / BATCH_SIZE))
        ]

    batches = get_batches("src", PYTHON_OPERATOR_SQL)

    bash_print = BashOperator.partial(
        task_id="bash_print", bash_command='echo "$batch_start -> $((batch_end - 1))"'
    ).expand(env=batches)

    @task
    def batch_select(conn_id, batch_data):
        batch_start = batch_data["batch_start"]
        sql = SQL_SELECT_BATCH.format(batch_start=batch_start)

        postgres_hook = PostgresHook(conn_id)
        df = postgres_hook.get_pandas_df(sql)
        logging.info(df)

    batch_select.partial(conn_id="src").expand(batch_data=batches)
