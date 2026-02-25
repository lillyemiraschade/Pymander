.PHONY: up down test lint migrate dev

up:
	docker compose up -d

down:
	docker compose down

test:
	uv run pytest -v

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

format:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

migrate:
	uv run alembic -c alembic/alembic.ini upgrade head

revision:
	uv run alembic -c alembic/alembic.ini revision --autogenerate -m "$(msg)"

dev:
	uv run uvicorn pymander.api.app:app --reload --host 0.0.0.0 --port 8000

topics:
	bash scripts/create_topics.sh
