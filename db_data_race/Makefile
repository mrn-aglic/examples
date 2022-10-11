run:
	docker-compose down && docker-compose up

restart:
	docker-compose stop && docker-compose start

clean-build-source:
	docker-compose build --no-cache --progress=plain source

clean-build-sink:
	docker-compose build --no-cache --progress=plain sink

remove-all:
	docker-compose down --rmi all --volumes
