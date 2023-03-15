import os

import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import XCom
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from sqlalchemy import and_, delete

HOURS_AGO = int(os.environ["HOURS_AGO"])


@provide_session
def cleanup_xcom(context, session=None):
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    delete_q = delete(XCom).filter(and_(XCom.dag_id == dag_id, XCom.run_id == run_id))
    session.execute(delete_q)


with DAG(
    dag_id="simple_task_group_mapping",
    start_date=pendulum.now().subtract(hours=HOURS_AGO),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
    on_success_callback=cleanup_xcom,
    description="This DAG demonstrates the use of task groups with dynamic task mapping using constant values",
    tags=["airflow2.5", "task_group_mapping"],
):

    @task_group(group_id="process_values")
    def process(element):
        @task
        def extract_number(el):
            return el["number"]

        @task
        def add_42(num):
            return num + 42

        add_42(extract_number(element))

    def _obtain_elements():
        return [
            {"number": 1},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    res = PythonOperator(task_id="obtain_elements", python_callable=_obtain_elements)

    def _sum_values(elements):
        print(f"elements: {elements}")
        return sum(elements)

    sum_values = PythonOperator(
        task_id="sum_values",
        python_callable=_sum_values,
        op_kwargs={"elements": "{{ ti.xcom_pull(task_ids='process_values.add_42') }}"},
    )

    process_node = process.expand(element=res.output)
    process_node >> sum_values
