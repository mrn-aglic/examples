import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session


def get_data():
    return [
        {"x": 1},
        {"x": 2},
        {"x": 3},
        {"x": 4},
        {"x": 5},
    ]


@provide_session
def on_success_callback(context, session=None):
    task_instance = context["ti"]

    print("------------")
    print("ON_SUCCESS_CALLBACK CALLED")
    print(task_instance.task_id)
    print(task_instance.map_index)
    print("------------")


with DAG(
    dag_id="callback_test",
    start_date=pendulum.now().subtract(hours=5),
    schedule="0 * * * *",
    description="This DAG demonstrates collecting weather data for multiple cities using dynamic task mapping",
    on_success_callback=on_success_callback,
    tags=["airflow2.5", "callback test"],
):
    gen = PythonOperator(task_id="generate_data", python_callable=get_data)

    test = PythonOperator.partial(
        task_id="test",
        python_callable=lambda x: print(f"TEST PYTHON OPERATOR: {x}"),
        on_success_callback=on_success_callback,
    ).expand(op_kwargs=gen.output)

    empty = EmptyOperator(task_id="empty")

    test >> empty
