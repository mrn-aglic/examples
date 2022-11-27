prep:
	sh ./prep.sh

cp-airflow22-ignores:
	cp airflowignores/airflowignore22 dags/.airflowignore

cp-airflow23-ignores:
	cp airflowignores/airflowignore23 dags/.airflowignore

cp-airflow24-ignores:
	cp airflowignores/airflowignore24 dags/.airflowignore

down:
	docker-compose down --volumes --remove-orphans && (rm dags/.airflowignore || true)

down-celery:
	docker-compose down -f docker-compose.feat23.celery.yml --volumes && (rm dags/.airflowignore || true)

run:
	make down && (rm dags/.airflowignores || true) && docker-compose up

run-2.2:
	make down && make cp-airflow22-ignores && docker-compose -f docker-compose.feat22.yml up

run-2.3:
	make down && make cp-airflow23-ignores && docker-compose -f docker-compose.yml up

run-2.3-celery:
	make down && make cp-airflow23-ignores && docker-compose -f docker-compose.feat23.celery.yml up --scale worker=3

run-2.4:
	make down && make cp-airflow24-ignores && docker-compose -f docker-compose.feat24.yml up

start-2.3:
	make cp-airflow23-ignores && docker-compose -f docker-compose.yml up

stop:
	docker-compose stop

restart-2.3:
	make stop && make start-2.3
