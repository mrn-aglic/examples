FROM apache/airflow:2.5.2-python3.10

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

COPY requirements ./requirements

RUN pip install -r ./requirements/requirements.txt
