.PHONY: run build clean

run:
	docker compose up

all:
	docker compose up --build

clean:
	docker compose down --rmi all --volumes