COMPOSE_FILE := infra/docker-compose.yml
# O compose vive em infra/, entao o Compose procuraria o .env de interpolacao em
# infra/. Apontamos explicitamente para o .env da raiz (fonte unica de variaveis).
ENV_FILE := .env
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

export PYTHONPATH := $(CURDIR):$(CURDIR)/dags:$(CURDIR)/plugins:$(CURDIR)/helpers
export MYPYPATH := $(CURDIR):$(CURDIR)/dags:$(CURDIR)/helpers:$(CURDIR)/plugins

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

# ---------------------------------------------------------------------------
# Ambiente local (infra/)
# ---------------------------------------------------------------------------

compose-config:
	$(COMPOSE) config

# Sobe em segundo plano, como o antigo `docker compose up -d` na raiz.
# Use `make up SERVICES=` para subir todos os servicos (inclui o jupyter).
SERVICES ?= postgres airflow superset

up:
	$(COMPOSE) up -d $(SERVICES)

down:
	$(COMPOSE) down

logs-airflow:
	$(COMPOSE) logs airflow --tail=200

# ---------------------------------------------------------------------------
# Site de documentacao (docs-pages/)
# ---------------------------------------------------------------------------

DOCS_DIR := $(CURDIR)/docs-pages
DOCS_VENV := $(DOCS_DIR)/.venv
DOCS_PY := $(DOCS_VENV)/bin/python
DOCS_RUN := PYTHONPATH=$(DOCS_DIR) $(DOCS_PY)

$(DOCS_PY):
	python3 -m venv $(DOCS_VENV)
	$(DOCS_VENV)/bin/pip install --quiet --upgrade pip jinja2 pyyaml markdown

docs-setup: $(DOCS_PY)

# Usa rede (git, gh) e grava o acervo versionado em docs-pages/src/_data/
docs-collect: $(DOCS_PY)
	$(DOCS_RUN) -m tooling.collect

# Roda offline, a partir do acervo ja coletado
docs-build: $(DOCS_PY)
	$(DOCS_RUN) -m tooling.build

docs-serve: docs-build
	@echo ""
	@echo "  Site local em http://localhost:8000  (Ctrl+C para parar)"
	@echo ""
	@$(DOCS_PY) -m http.server 8000 --directory $(DOCS_DIR)/site

docs-clean:
	rm -rf $(DOCS_DIR)/site

.PHONY: setup format lint lint-ci test compose-config up down logs-airflow \
	docs-setup docs-collect docs-build docs-serve docs-clean
