prep:
	sh ./prep.sh

cp-airflow22-ignores:
	cp airflowignores/airflowignore22 dags/.airflowignore

cp-airflow23-ignores:
	cp airflowignores/airflowignore23 dags/.airflowignore

cp-airflow24-ignores:
	cp airflowignores/airflowignore24 dags/.airflowignore

down:
	docker-compose down --volumes && (rm dags/.airflowignore || true)

run:
	make down && make cp-airflow24-ignores && docker-compose up

run-2.2:
	make down && make cp-airflow22-ignores && docker-compose -f docker-compose.feat22.yml up

run-2.3:
	make down && make cp-airflow23-ignores && docker-compose -f docker-compose.yml up

run-2.4:
	make down && make cp-airflow24-ignores && docker-compose -f docker-compose.feat24.yml up
