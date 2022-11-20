import logging
import os

import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator
from airflow.settings import AIRFLOW_HOME
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

# From section (1st param) in config get key (2nd param)
worker_container_repository = conf.get(
    "kubernetes_executor", "worker_container_repository"
)

worker_container_tag = conf.get("kubernetes_executor", "worker_container_tag")

start_task_executor_config = {
    "pod_template_file": os.path.join(
        AIRFLOW_HOME, "pod_templates/basic_template.yaml"
    ),
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"})
    ),
}


def _print_f(**context):
    print(context)


with DAG(
    dag_id="local_kubernetes_executor",
    start_date=pendulum.now().subtract(int(os.environ["HOURS_AGO"])),
    schedule=None,
    tags=["airflow2.3", "executor"],
):
    print_op = PythonOperator(
        task_id="print_task",
        python_callable=_print_f,
    )
