COMPOSE_FILE := infra/docker-compose.yml
# O compose vive em infra/, entao o Compose procuraria o .env de interpolacao em
# infra/. Apontamos explicitamente para o .env da raiz (fonte unica de variaveis).
ENV_FILE := .env
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

export PYTHONPATH := $(CURDIR)/dags:$(CURDIR)/plugins:$(CURDIR)/helpers
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

# Nunca publique `dbt docs generate` diretamente: manifest/catalog completos
# incluem bronze e SQL compilado. Este alvo usa diretório temporário privado e
# publica exclusivamente o catálogo de metadados permitido.
conjuntura-docs:
	poetry run dbt deps --project-dir dbt/mcid
	poetry run python scripts/conjuntura/gerar_docs_seguros.py

conjuntura-docs-pdf: conjuntura-docs
	soffice --headless --convert-to pdf --outdir build build/pipeline.html

# Catálogo de fontes: um bloco por quadro do boletim, com o endereço da
# origem clicável. O PDF sai pelo Chrome, e não pelo soffice, porque só ele
# preserva a anotação de link — no soffice a URL vira texto morto.
conjuntura-catalogo:
	poetry run python scripts/conjuntura/gerar_catalogo_fontes.py \
		--saida build/catalogo-fontes.html
	printf '<!doctype html><html lang="pt-BR" data-theme="light"><head><meta charset="utf-8">' > build/catalogo-fontes-impressao.html
	cat build/catalogo-fontes.html >> build/catalogo-fontes-impressao.html
	printf '</html>' >> build/catalogo-fontes-impressao.html
	google-chrome-stable --headless --disable-gpu --no-sandbox \
		--print-to-pdf=build/Catalogo-Fontes-Conjuntura-Habitacional.pdf \
		--no-pdf-header-footer file://$$PWD/build/catalogo-fontes-impressao.html

# Audita somente descrições YAML e não acessa tabelas ou arquivos de dados.
# Use `GOVERNANCE_STRICT=--strict make governance-audit` ao transformar os
# achados em gate de CI.
governance-audit:
	poetry run python scripts/governance/auditar_metadados.py $(GOVERNANCE_STRICT)

# Executa no GX os contratos Silver declarados no YAML do dbt. O relatório é
# sanitizado e não contém linhas nem valores inesperados.
gx-silver:
	poetry run python scripts/governance/validar_silver_gx.py $(GX_STRICT)

governance-load-strategies:
	poetry run python scripts/governance/auditar_estrategias_carga.py $(GOVERNANCE_STRICT)

# Gera docs dbt em diretório temporário privado e persiste somente a projeção
# semântica filtrada de Silver/Gold.
openmetadata-catalog:
	poetry run python scripts/governance/gerar_catalogo_openmetadata_seguro.py

# Estrutura: schema, tabela, coluna e linhagem. Cria o payload seguro para
# OpenMetadata. Acrescente `--confirmar` em OPENMETADATA_ARGS somente após
# preencher URL e token no .env (o modelo está em infra/env/.env.example).
openmetadata-sync: openmetadata-catalog
	poetry run python scripts/governance/sincronizar_openmetadata.py $(OPENMETADATA_ARGS)

# Governança: domínio, produto de dados, proprietário, classificação, etiqueta,
# tier, certificação, permissão de uso e glossário. Sem URL/token no ambiente
# roda offline e só imprime o que está declarado.
openmetadata-governanca:
	poetry run python scripts/governance/sincronizar_governanca.py $(OPENMETADATA_ARGS)

# A ordem importa: não se pendura domínio, produto nem etiqueta em tabela que
# ainda não existe no catálogo. Estrutura primeiro, governança depois. E
# reescrever as colunas substitui o array inteiro, levando junto a etiqueta de
# glossário que a governança pendura nelas — por isso nunca o contrário.
openmetadata: openmetadata-sync openmetadata-governanca openmetadata-lake

# Data lake e orquestração: MinIO como serviço de armazenamento, cada parquet
# como container, e a linhagem DAG -> parquet -> Bronze. Roda por último: liga
# containers a tabelas que precisam existir antes.
openmetadata-lake:
	poetry run python scripts/governance/sincronizar_lake.py $(OPENMETADATA_ARGS)

# Confere o que está NA INSTÂNCIA contra o que o repo declara. O
# `governance-audit` audita se a documentação foi escrita; este audita se ela
# chegou. A distância entre as duas já foi de 85 tabelas.
# Use `OPENMETADATA_AUDIT=--strict` para transformar em gate de CI.
governance-audit-om:
	poetry run python scripts/governance/auditar_openmetadata.py $(OPENMETADATA_AUDIT)

# A conferência contra os boletins publicados virou teste do dbt: roda no
# mesmo `dbt build` que já roda, em vez de script com conexão própria.
conjuntura-validar-boletins:
	cd dbt/mcid && poetry run dbt seed --select boletim_gabarito \
		&& poetry run dbt test --select conjuntura_gabarito_do_boletim

# Congela as edições do boletim (as fontes revisam o passado).
# É `dbt snapshot`, não script: o snapshot guarda o HISTÓRICO das revisões,
# não um retrato, e é o que responde "por que o boletim publicou X e hoje é Y".
conjuntura-congelar:
	cd dbt/mcid && poetry run dbt snapshot

.PHONY: setup format lint lint-ci test compose-config up down logs-airflow \
	docs-setup docs-collect docs-build docs-serve docs-clean conjuntura-docs \
	conjuntura-docs-pdf conjuntura-catalogo conjuntura-validar-boletins \
	conjuntura-congelar \
	governance-audit gx-silver governance-load-strategies openmetadata-catalog \
	openmetadata-sync openmetadata-governanca openmetadata-lake openmetadata \
	governance-audit-om
