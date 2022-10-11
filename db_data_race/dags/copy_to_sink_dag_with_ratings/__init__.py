import logging
import os
from tempfile import NamedTemporaryFile
from typing import Any

import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

TABLE = os.environ["PG_TABLE_NAME"]


class TransferPsqlToMysql(BaseOperator):

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


with DAG(
    dag_id="copy_to_sink_with_ratings",
    description="DAG demonstrating a data race condition.",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule_interval="0 * * * *",
) as dag:
    copy_to_mysql = TransferPsqlToMysql(
        task_id="postgres_to_mysql",
        postgres_conn_id="src",
        mysql_conn_id="mysql",
        dst_table_name="movies",
        sql=f"SELECT imdbId, id, title FROM {TABLE}",
        truncate_table=True,
    )

    aggregate_data = MySqlOperator(
        task_id="aggregate_data", mysql_conn_id="mysql", sql="sql/aggregate_data.sql"
    )

    copy_to_mysql >> aggregate_data
