import logging
import math
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PYTHON_OPERATOR_SQL = "SELECT COUNT(*) as CNT FROM match_scores"
BATCH_SIZE = 80
SQL_SELECT_BATCH = (
    f"SELECT * FROM match_scores OFFSET {{batch_start}} LIMIT {BATCH_SIZE}"
)


def get_batches(conn_id, sql, **context):
    postgres_hook = PostgresHook(conn_id)
    rows = postgres_hook.get_records(sql)
    count = rows[0][0]

    return [
        {"batch_start": str(i * BATCH_SIZE), "batch_end": str((i + 1) * BATCH_SIZE)}
        for i in range(math.ceil(count / BATCH_SIZE))
    ]


def batch_select(conn_id, batch_data, **context):
    logging.info(conn_id)
    logging.info(batch_data)
    batch_start = batch_data["batch_start"]
    sql = SQL_SELECT_BATCH.format(batch_start=batch_start)

    postgres_hook = PostgresHook(conn_id)
    df = postgres_hook.get_pandas_df(sql)
    logging.info(df)


with DAG(
    dag_id="task_mapping_classic_operators",
    description="DAG demonstrating a data race condition.",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule_interval="0 * * * *",
) as dag:
    get_batch_starts = PythonOperator(
        task_id="get_batch_starts",
        op_kwargs={
            "conn_id": "src",
            "sql": PYTHON_OPERATOR_SQL,
            "batch_size": BATCH_SIZE,
        },
        python_callable=get_batches,
    )

    bash_print = BashOperator.partial(
        task_id="bash_print", bash_command='echo "$batch_start -> $((batch_end - 1))"'
    ).expand(env=get_batch_starts.output)

    def prep_args(batch_data):
        return {"conn_id": "src", "batch_data": batch_data}

    batch_select_op = PythonOperator.partial(
        task_id="batch_select", python_callable=batch_select
    ).expand(op_kwargs=get_batch_starts.output.map(prep_args))

    # The postgres operator would probably look something like this (we'll see):
    # batch_select_op = PostgresOperator.partial(
    #     task_id="batch_select",
    #     postgres_conn_id="src",
    #     sql=f"SELECT * FROM match_scores OFFSET {{{{ task.expand_input.value.parameters.batch_start }}}} LIMIT {BATCH_SIZE}"
    # ).expand(parameters=get_batch_starts.output)
