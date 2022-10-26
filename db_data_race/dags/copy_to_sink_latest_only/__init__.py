import logging
import os
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

TABLE = os.environ["PG_TABLE_NAME"]


class TransferPsqlToMySql(BaseOperator):

    template_fields = ["sql"]
    template_ext = [".sql"]
    template_fields_renderers = {"sql": "sql"}

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        postgres_conn_id,
        mysql_conn_id,
        sql,
        dst_table_name,
        truncate_table,
        **kwargs,
    ):
        self.postgres_conn_id = postgres_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.dst_table_name = dst_table_name
        self.truncate_table = truncate_table
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        postgres_hook = PostgresHook(self.postgres_conn_id)
        mysql_hook = MySqlHook(self.mysql_conn_id)

        with NamedTemporaryFile("w+b") as file:
            logging.info("Exporting data to csv file")
            postgres_hook.copy_expert(
                f"COPY ({self.sql}) TO STDOUT WITH (FORMAT csv, DELIMITER '\t')",
                file.name,
            )
            file.flush()
            file.seek(0)

            if self.truncate_table:
                mysql_hook.run(f"TRUNCATE TABLE {self.dst_table_name}")

            mysql_hook.bulk_load(self.dst_table_name, file.name)


def replace_data(conn_id, query_file, **context):
    airflow_home = os.getenv("AIRFLOW_HOME")
    with open(f"{airflow_home}/dags/{query_file}", "r") as file:
        sql = file.read()

    mysql_hook = MySqlHook(conn_id)

    chunksize = 100

    with NamedTemporaryFile("w+b") as file:

        header = True
        for chunk_df in mysql_hook.get_pandas_df_by_chunks(sql, chunksize=chunksize):
            chunk_df.to_csv(file.name, mode="a", index=False, header=header)
            header = False

        mysql_hook.run("TRUNCATE TABLE movies")

        engine = mysql_hook.get_sqlalchemy_engine()
        for chunk_df in pd.read_csv(file.name, chunksize=chunksize):
            chunk_df.to_sql("movies", con=engine, if_exists="append", index=False)


with DAG(
    dag_id="copy_to_sink_latest_only",
    description="DAG demonstrating a data race condition.",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule_interval="0 * * * *",
) as dag:
    copy_to_mysql = TransferPsqlToMySql(
        task_id="postgres_to_mysql",
        postgres_conn_id="src",
        mysql_conn_id="mysql",
        dst_table_name="movies",
        sql=f"SELECT imdbId, id, title FROM {TABLE}",
        truncate_table=True,
    )

    cleanup_data = MySqlOperator(
        task_id="cleanup_data",
        mysql_conn_id="mysql",
        sql="DELETE FROM movie_ratings WHERE logical_date = '{{ ts }}'",
    )

    aggregate_data = MySqlOperator(
        task_id="aggregate_data",
        mysql_conn_id="mysql",
        sql="sql/aggregate_data_window.sql",
    )

    latest_only = LatestOnlyOperator(task_id="latest_only", depends_on_past=True)

    cleanup_temp_table = PythonOperator(
        task_id="remove_duplicates_from_temp",
        op_kwargs={
            "conn_id": "mysql",
            "query_file": "copy_to_sink_latest_only/sql/get_unique_movies.sql",
        },
        python_callable=replace_data,
    )

    copy_to_mysql >> cleanup_data >> aggregate_data >> latest_only >> cleanup_temp_table
