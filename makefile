.PHONY: run build clean

run:
	docker compose up

build:
	docker compose up --build

clean:
	docker compose down --rmi all --volumes