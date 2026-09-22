# AI Agent Swarm — developer entry points.
COMPOSE := docker compose -f docker/compose.yaml
UV      := python -m uv

.PHONY: help up down logs topics reset install test test-unit test-int lint fmt typecheck check console

help:
	@echo "up          start redpanda, console, postgres, redis"
	@echo "down        stop the stack (keeps volumes)"
	@echo "reset       stop the stack and delete its data"
	@echo "topics      list broker topics"
	@echo "console     open Redpanda Console"
	@echo "install     sync the uv workspace"
	@echo "test        unit + integration (needs 'make up')"
	@echo "test-unit   unit tests only, no containers"
	@echo "check       lint, format check, types, unit tests"

up:
	$(COMPOSE) up -d
	@echo "console: http://localhost:$${CONSOLE_PORT:-8080}"

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

topics:
	docker exec swarm-redpanda rpk topic list

console:
	@python -c "import webbrowser,os; webbrowser.open(f'http://localhost:{os.environ.get(\"CONSOLE_PORT\",\"8080\")}')"

install:
	$(UV) sync

test:
	$(UV) run pytest -q

test-unit:
	$(UV) run pytest -q -m "not integration"

test-int:
	$(UV) run pytest -q -m integration

lint:
	$(UV) run ruff check .

fmt:
	$(UV) run ruff format .

typecheck:
	$(UV) run mypy packages apps

check: lint test-unit
	$(UV) run ruff format --check .
