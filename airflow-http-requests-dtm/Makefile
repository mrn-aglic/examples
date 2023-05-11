clean:
	docker compose down --volumes --rmi "all"

down:
	docker compose down --volumes

run:
	make down && docker compose up

build-run:
	make down && docker compose up --build
