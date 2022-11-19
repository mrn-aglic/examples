FROM apache/airflow:2.4.2-python3.10

COPY requirements ./requirements

RUN pip install --upgrade pip
RUN pip install -r ./requirements/requirements.txt
