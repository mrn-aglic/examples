import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="pull_specific_indexes",
    start_date=pendulum.now().subtract(hours=int(os.environ["HOURS_AGO"])),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
    description="This DAG demonstrates how to pull specific XCom indexes",
    tags=["airflow2.5", "task_mapping", "pull_specific_indexes"],
):

    def _obtain_elements():
        return [
            {"number": 17},
            {"number": 2},
            {"number": 3},
            {"number": 4},
            {"number": 5},
        ]

    res = PythonOperator(task_id="obtain_elements", python_callable=_obtain_elements)

    def _mapped_tasks(number, **context):
        task_instance = context["ti"]
        map_index = task_instance.map_index
        print(f"{task_instance.task_id}, index {map_index} got element: {number}")
        return number

    to_map = PythonOperator.partial(
        task_id="mapped",
        python_callable=_mapped_tasks,
    ).expand(op_kwargs=res.output)

    def _puller(**context):
        task_instance = context["ti"]
        elements = task_instance.xcom_pull(
            task_ids="mapped",
            map_indexes=[1, 4],
        )
        print(f"Got elements: {elements}")

    puller = PythonOperator(
        task_id="puller",
        python_callable=_puller,
    )

    def _puller_ninja(elements):
        print(f"Got elements: {elements}")

    def _puller_ninja_test(*elements):
        print(f"Got elements: {elements}")

    puller_ninja = PythonOperator(
        task_id="puller_ninja",
        python_callable=_puller_ninja,
        op_kwargs={
            "elements": "{{ ti.xcom_pull(task_ids='mapped', map_indexes=[2, 3]) }}"
        },
    )

    # Try to use map_indexes with a non-mapped operator
    # res >> PythonOperator(
    #     task_id="puller_ninja_test",
    #     python_callable=_puller_ninja,
    #     op_kwargs={"elements": "{{ ti.xcom_pull(task_ids='mapped', map_indexes=[0]) }}"}
    # )

    to_map >> [puller, puller_ninja]
