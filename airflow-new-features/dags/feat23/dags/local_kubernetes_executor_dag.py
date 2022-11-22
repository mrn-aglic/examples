import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

start_task_executor_config = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"})
    ),
}


def _print_f(queue, **context):
    logger.info("Executor running: %s", queue)
    logger.info(context)


with DAG(
    dag_id="local_kubernetes_executor",
    start_date=pendulum.now().subtract(minutes=15),
    schedule=timedelta(minutes=5),
    tags=["airflow2.3", "executor"],
):
    print_op_kubernetes = PythonOperator(
        task_id="print_task_with_kubernetes",
        python_callable=_print_f,
        op_kwargs={"queue": "kubernetes"},
        executor_config=start_task_executor_config,
        queue="kubernetes",
    )

    print_op_local = PythonOperator(
        task_id="print_task_with_local",
        python_callable=_print_f,
        op_kwargs={"queue": "local"},
    )

    print_op_local >> print_op_kubernetes
