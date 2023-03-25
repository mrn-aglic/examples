from airflow.models import XCom
from airflow.utils.session import provide_session
from sqlalchemy import and_, delete


@provide_session
def cleanup_xcom(context, session=None):
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    task_instance = context["ti"]
    map_index = task_instance.map_index

    delete_q = delete(XCom).filter(
        and_(XCom.dag_id == dag_id, XCom.run_id == run_id, XCom.map_index == map_index)
    )
    session.execute(delete_q)
