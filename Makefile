.PHONY: up down test lint
up:
	docker compose -f docker/docker-compose.yml up -d
down:
	docker compose -f docker/docker-compose.yml down
test:
	pytest tests/ -v
lint:
	ruff check .
