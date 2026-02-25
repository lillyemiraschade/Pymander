.PHONY: up down test lint migrate dev app topics dashboard

# Infrastructure
up:
	docker compose up -d

down:
	docker compose down

# App services (requires .env with credentials)
app:
	docker compose --profile app up -d

app-down:
	docker compose --profile app down

# Development
test:
	uv run pytest -v

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

format:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

# Database
migrate:
	uv run alembic -c alembic/alembic.ini upgrade head

revision:
	uv run alembic -c alembic/alembic.ini revision --autogenerate -m "$(msg)"

# Local dev server
dev:
	uv run uvicorn pymander.api.app:app --reload --host 0.0.0.0 --port 8000

# Kafka topics
topics:
	bash scripts/create_topics.sh

# Dashboard
dashboard-dev:
	cd dashboard && npm run dev

dashboard-build:
	cd dashboard && npm run build
