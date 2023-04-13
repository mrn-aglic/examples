from airflow.models import XCom
from airflow.utils.session import provide_session
from sqlalchemy import and_, delete


@provide_session
def cleanup_xcom(context, session=None):
    dag_run = context["dag_run"]
    task_instance = context["ti"]

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    map_index = task_instance.map_index
    task_id = task_instance.task_id

    delete_q = delete(XCom).filter(
        and_(
            XCom.dag_id == dag_id,
            XCom.run_id == run_id,
            XCom.task_id == task_id,
            XCom.map_index == map_index,
        )
    )

    session.execute(delete_q)


@provide_session
def cleanup_xcom_dag(context, session=None):
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    delete_q = delete(XCom).filter(and_(XCom.dag_id == dag_id, XCom.run_id == run_id))

    session.execute(delete_q)


def cleanup_xcom_of_previous_tasks(context):
    dag_run = context["dag_run"]
    task_instance = context["ti"]

    task = context["task"]

    task_instances = [
        dag_run.get_task_instance(tid, map_index=task_instance.map_index)
        for tid in task.upstream_task_ids
    ]

    for ti in task_instances:
        ti.clear_xcom_data()
