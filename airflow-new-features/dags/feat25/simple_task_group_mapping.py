import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="simple_task_group_mapping",
    start_date=pendulum.now().subtract(hours=1),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
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
        # [print(type(a)) for a in elements]

        return sum(elements)

    sum_values = PythonOperator(
        task_id="sum_values",
        python_callable=_sum_values,
        op_kwargs={"elements": "{{ ti.xcom_pull(task_ids='process_values.add_42') }}"},
    )

    process_node = process.expand(element=res.output)
    process_node >> sum_values
