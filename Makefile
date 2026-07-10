COMPOSE_FILE := infra/docker-compose.yml
# O compose vive em infra/, entao o Compose procuraria o .env de interpolacao em
# infra/. Apontamos explicitamente para o .env da raiz (fonte unica de variaveis).
ENV_FILE := .env
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

export PYTHONPATH := $(CURDIR)/dags:$(CURDIR)/plugins:$(CURDIR)/helpers
export MYPYPATH := $(CURDIR):$(CURDIR)/dags:$(CURDIR)/helpers:$(CURDIR)/plugins

.PHONY: setup format lint lint-ci test compose-config up down logs-airflow

setup:
	pip install poetry==1.8.5
	poetry config virtualenvs.in-project false
	poetry config warnings.export false
	poetry lock
	poetry install --no-root --with dev
	poetry export --without-hashes --format=requirements.txt > requirements.generated.txt
	bash setup-git-hooks.sh

format:
	poetry run black .
	poetry run ruff check --fix .
	poetry run sqlfmt ./dbt

lint:
	poetry run black . --check
	poetry run ruff check .
	poetry run mypy . --explicit-package-bases --install-types --non-interactive
	poetry run sqlfmt ./dbt --check
	[ "${GITLAB_CI}" ] || poetry run sqlfluff lint ./dbt

lint-ci:
	poetry run sqlfmt ./dbt --check
	poetry run sqlfluff lint ./dbt --config .sqlfluff.ci --ignore templating

test:
	poetry run pytest tests

compose-config:
	$(COMPOSE) config

up:
	$(COMPOSE) up postgres airflow superset

down:
	$(COMPOSE) down

logs-airflow:
	$(COMPOSE) logs airflow --tail=200
