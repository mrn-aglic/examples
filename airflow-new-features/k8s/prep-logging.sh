#!/bins/sh

mc alias set minio-airflow http://host.docker.internal:9000 minioadmin minioadmin
mc mb minio-airflow/airflow-logs
