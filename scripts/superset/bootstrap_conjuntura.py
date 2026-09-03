#!/usr/bin/env python3
"""Cria, de forma idempotente, os datasets, charts e dashboards da Conjuntura.

Uso:
    python scripts/superset/bootstrap_conjuntura.py --dry-run
    python scripts/superset/bootstrap_conjuntura.py --with-charts

As credenciais são lidas somente de ``.env``/variáveis de ambiente. O script
nunca imprime senhas ou a URI SQLAlchemy.
"""

from __future__ import annotations

import argparse
import json
import re
import os
from dataclasses import dataclass
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv

GOLD_SCHEMA = "conjuntura"
DATABASE_NAME = "Cidades"

GOLDS = [
    "gld_balancos_empresas",
    "gld_balancos_empresas_totais",
    "gld_canal_fgts",
    "gld_credito_pib",
    "gld_empregos_caged",
    "gld_fgts_valor_medio",
    "gld_financiamento_pf_faixa",
    "gld_financiamentos_habitacionais",
    "gld_financiamentos_imobiliarios_pf_pj",
    "gld_financiamentos_instituicao",
    "gld_fipezap",
    "gld_funding",
    "gld_icst",
    "gld_incc_m",
    "gld_indice_imob",
    "gld_novos_financiamentos_banco",
    "gld_ogu",
    "gld_pib_construcao_civil",
    "gld_pib_construcao_civil_pct",
    "gld_pnad_ocupados",
    "gld_pnad_rendimento",
    "gld_producao_fisica",
    "gld_saldo_poupanca",
    "gld_sinapi",
    "gld_ticket_medio",
    "gld_uh_condicao_uso",
    "gld_fundo_social",
]

#: Estrutura do Boletim de Conjuntura, página a página. A ordem aqui é a ordem
#: impressa: as abas do dashboard reproduzem o boletim para que a conferência
#: manual contra o PDF seja lado a lado, sem procurar indicador.
#:
#: Fonte: validação indicador a indicador do boletim (documentação interna), que
#: mapeia cada item publicado ao seu gold.
PAGINAS_BOLETIM: list[tuple[str, list[str]]] = [
    (
        "Pág. 1 — PIB da Construção Civil",
        [
            "gld_pib_construcao_civil_pct",
            "gld_pib_construcao_civil",
        ],
    ),
    (
        "Pág. 2 — Balanço das Empresas · Financiamentos Imobiliários",
        [
            "gld_balancos_empresas",
            "gld_balancos_empresas_totais",
            "gld_financiamentos_imobiliarios_pf_pj",
            "gld_financiamentos_habitacionais",
        ],
    ),
    (
        "Pág. 3 — Empregos · PNAD · Produção Física · Novos Financiamentos",
        [
            "gld_empregos_caged",
            "gld_pnad_ocupados",
            "gld_pnad_rendimento",
            "gld_producao_fisica",
            "gld_novos_financiamentos_banco",
            # Sucessor de `novos_financiamentos_banco`: mesmo indicador do
            # boletim, fonte nova. Convivem até a aposentadoria do antigo.
            "gld_financiamentos_instituicao",
        ],
    ),
    (
        "Pág. 4 — Crédito/PIB · Faixa de Renda · Condição de Uso · Funding",
        [
            "gld_credito_pib",
            "gld_financiamento_pf_faixa",
            "gld_uh_condicao_uso",
            "gld_funding",
        ],
    ),
    (
        "Pág. 5 — Canal FGTS · Poupança",
        [
            "gld_canal_fgts",
            "gld_saldo_poupanca",
        ],
    ),
    (
        "Pág. 6 — OGU · Preços",
        [
            "gld_ogu",
            "gld_sinapi",
            "gld_incc_m",
            "gld_ticket_medio",
            "gld_fgts_valor_medio",
        ],
    ),
    (
        "Pág. 7 — Índices da Construção",
        [
            "gld_indice_imob",
            "gld_fipezap",
            "gld_icst",
        ],
    ),
    (
        # Aba explicitamente fora do boletim impresso: nada aqui tem página
        # correspondente no PDF, e misturar com as sete acima faria a
        # conferência lado a lado perder o sentido.
        "Complementos (fora do boletim impresso)",
        [
            "gld_fundo_social",
        ],
    ),
]

TEMPORAL_COLUMN = {
    "gld_credito_pib": "data",
    "gld_financiamentos_imobiliarios_pf_pj": "data",
    "gld_incc_m": "mes",
}


@dataclass(frozen=True)
class Dashboard:
    title: str
    slug: str
    description: str
    time_range: str


DASHBOARDS = [
    # Um dashboard por trimestre já existiu aqui — 2026.1, 2026.2, 2026.3 —,
    # três cópias do mesmo conjunto de charts que só diferiam no
    # `default_filters`. É duplicação: o recorte por trimestre é FILTRO, não
    # dashboard. O `build_boletim.py` já monta assim, num dashboard só com
    # filtro nativo na coluna `edicao`. Recriá-los aqui ressuscitava painéis
    # que a equipe tinha decidido não manter.
    Dashboard(
        "Conjuntura Habitacional — Contínuo",
        "conjuntura-continuo",
        "Série contínua, mensal e trimestral, do setor habitacional.",
        "No filter",
    ),
]


class Superset:
    def __init__(
        self, base_url: str, username: str, password: str, dry_run: bool
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.session = requests.Session()
        # Autentica sempre, inclusive em dry-run: o dry-run precisa LER o
        # estado atual para dizer o que criaria de fato. Só as escritas
        # continuam puladas.
        #
        # Login por FORMULÁRIO, não por JWT. Nesta instância o bearer é cego
        # para os dashboards da conjuntura — `GET /api/v1/dashboard/<slug>`
        # devolve 404 com bearer e 200 com o cookie de sessão, e a listagem
        # com bearer omite os mesmos dashboards. Foi isso que fez o script
        # relatar "já existente e preservado" para um dashboard que ele
        # poderia (e precisava) atualizar. O cookie enxerga estritamente mais,
        # então é o único mecanismo usado aqui.
        pagina = self.session.get(f"{self.base_url}/login/", timeout=30)
        pagina.raise_for_status()
        achado = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', pagina.text)
        self.session.post(
            f"{self.base_url}/login/",
            data={
                "username": username,
                "password": password,
                "csrf_token": achado.group(1) if achado else "",
            },
            timeout=30,
        ).raise_for_status()
        csrf = self.session.get(
            f"{self.base_url}/api/v1/security/csrf_token/", timeout=30
        )
        csrf.raise_for_status()
        self.session.headers["X-CSRFToken"] = csrf.json()["result"]
        self.session.headers["Referer"] = self.base_url

    def list(self, resource: str) -> list[dict]:
        # Listar é leitura: roda também em dry-run. Antes retornava lista
        # vazia, o que fazia o dry-run relatar que criaria TODOS os recursos,
        # mesmo os já existentes — um dry-run que não consegue dizer o que
        # vai acontecer não serve para nada. Só as escritas são puladas.
        # A API ignora `page`/`page_size` soltos e então devolve só 20 itens.
        # `q` é Rison, o formato documentado pelo Superset para paginação.
        pagina, tamanho, resultado = 0, 100, []
        while True:
            response = self.session.get(
                f"{self.base_url}/api/v1/{resource}/",
                params={"q": f"(page:{pagina},page_size:{tamanho})"},
                timeout=30,
            )
            response.raise_for_status()
            corpo = response.json()
            lote = corpo.get("result", [])
            resultado.extend(lote)
            if len(resultado) >= corpo.get("count", 0) or not lote:
                return resultado
            pagina += 1

    def create(self, resource: str, payload: dict) -> dict:
        if self.dry_run:
            print(
                f"[dry-run] criaria {resource}: {payload.get('database_name') or payload.get('table_name') or payload.get('slice_name') or payload.get('dashboard_title')}"
            )
            return {"id": -1}
        response = self.session.post(
            f"{self.base_url}/api/v1/{resource}/", json=payload, timeout=30
        )
        if not response.ok:
            raise RuntimeError(
                f"Superset recusou criar {resource} (HTTP {response.status_code}): "
                f"{response.text}"
            )
        response.raise_for_status()
        body = response.json()
        if body.get("id") is not None:
            return {"id": body["id"]}
        result = body.get("result")
        if isinstance(result, dict) and result.get("id") is not None:
            return result
        raise RuntimeError(
            f"Superset não retornou id ao criar {resource}: {json.dumps(body, ensure_ascii=False)}"
        )

    def update(self, resource: str, resource_id: int, payload: dict) -> None:
        if self.dry_run:
            return
        response = self.session.put(
            f"{self.base_url}/api/v1/{resource}/{resource_id}", json=payload, timeout=30
        )
        response.raise_for_status()

    def dashboard_id(self, slug: str) -> int | None:
        """Resolve o id de um dashboard pelo slug.

        A listagem de `/api/v1/dashboard/` não revela os dashboards da
        conjuntura, mas a busca direta por slug revela. Devolver o id (e não
        um booleano de existência) é o que permite atualizar o dashboard em
        vez de apenas constatar que ele existe e deixá-lo como está.
        """
        response = self.session.get(
            f"{self.base_url}/api/v1/dashboard/{slug}", timeout=30
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["result"]["id"]


def env(name: str, *alternativos: str) -> str:
    """Lê a primeira variável definida entre `name` e seus sinônimos.

    A URL do Superset tem dois nomes no projeto: estes scripts sempre pediram
    `SUPERSET_URL`, enquanto a integração do OpenMetadata e o `.env.example`
    declaram `SUPERSET_HOST_PORT`. Quem montava o ambiente pelo exemplo não
    conseguia rodar os scripts. Aceitar os dois evita a pegadinha sem quebrar
    quem já tem o `.env` antigo.
    """
    for candidato in (name, *alternativos):
        value = os.getenv(candidato)
        if value:
            return value
    nomes = " ou ".join((name, *alternativos))
    raise SystemExit(f"Variável obrigatória ausente: {nomes}")


def get_or_create_database(api: Superset) -> int:
    for database in api.list("database"):
        if database.get("database_name") == DATABASE_NAME:
            return database["id"]
    uri = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
        user=quote_plus(env("DB_DW_USER_MCID")),
        password=quote_plus(env("DB_DW_PASSWORD_MCID")),
        host=env("DB_DW_HOST_MCID"),
        port=env("DB_DW_PORT_MCID"),
        database=env("DB_DW_DBNAME_MCID"),
    )
    result = api.create(
        "database",
        {
            "database_name": DATABASE_NAME,
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        },
    )
    return result["id"] if isinstance(result, dict) else result


def get_or_create_datasets(api: Superset, database_id: int) -> dict[str, int]:
    existing = {
        (item.get("schema"), item.get("table_name")): item["id"]
        for item in api.list("dataset")
    }
    result: dict[str, int] = {}
    for gold in GOLDS:
        key = (GOLD_SCHEMA, gold)
        if key not in existing:
            created = api.create(
                "dataset",
                {"database": database_id, "schema": GOLD_SCHEMA, "table_name": gold},
            )
            existing[key] = created["id"] if isinstance(created, dict) else created
        result[gold] = existing[key]
    return result


def get_or_create_charts(api: Superset, datasets: dict[str, int]) -> list[int]:
    """Cria os charts que faltam e REAPONTA os que ficaram órfãos.

    Guardar só o id do chart existente não bastava: quando o dataset muda de
    lugar — foi o que aconteceu ao unificar os schemas do conjuntura, com
    `conjuntura_mart.gold_continuo_*` virando `conjuntura.gld_*` — o chart
    continua existindo com o mesmo nome, apontando para um dataset que já não
    existe. O chart não some da tela; ele simplesmente para de carregar. Por
    isso comparamos também o dataset atual, e corrigimos quando divergir.
    """
    existing = {
        item.get("slice_name"): (item["id"], item.get("datasource_id"))
        for item in api.list("chart")
    }
    ids: list[int] = []
    for gold, dataset_id in datasets.items():
        title = nome_do_chart(gold)
        params = {
            "datasource": f"{dataset_id}__table",
            "viz_type": "table",
            "query_mode": "aggregate",
            "all_columns": [],
            "groupby": [],
            "metrics": [],
            "row_limit": 1000,
        }
        if title not in existing:
            created = api.create(
                "chart",
                {
                    "slice_name": title,
                    "viz_type": "table",
                    "datasource_id": dataset_id,
                    "datasource_type": "table",
                    "params": json.dumps(params),
                },
            )
            novo_id = created["id"] if isinstance(created, dict) else created
            existing[title] = (novo_id, dataset_id)
        else:
            chart_id, dataset_atual = existing[title]
            if dataset_atual != dataset_id:
                print(
                    f"{'[dry-run] ' if api.dry_run else ''}reaponta chart "
                    f"{title!r}: dataset {dataset_atual} -> {dataset_id}"
                )
                api.update(
                    "chart",
                    chart_id,
                    {
                        "datasource_id": dataset_id,
                        "datasource_type": "table",
                        "params": json.dumps(params),
                    },
                )
                existing[title] = (chart_id, dataset_id)
        ids.append(existing[title][0])
    return ids


def dashboard_position(chart_ids: list[int]) -> str:
    """Gera layout nativo do Superset: ROOT → GRID → ROW → CHART.

    O frontend do Superset 6 acessa ``node.meta.width`` durante a montagem.
    Ligar charts diretamente ao ROOT ou omitir ``meta`` nos nós estruturais
    causa o erro ``n.meta is undefined``.
    """
    layout: dict[str, dict] = {
        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": ["GRID_ID"],
            "parents": [],
            "meta": {},
        },
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
    }
    for index, chart_id in enumerate(chart_ids):
        row_id = f"ROW-{index:03d}"
        node_id = f"CHART-{chart_id}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "children": [node_id],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[node_id] = {
            "id": node_id,
            "type": "CHART",
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "children": [],
            "meta": {
                "chartId": chart_id,
                "width": 12,
                "height": 50,
                "index": f"{index:03d}",
            },
        }
    return json.dumps(layout)


def dashboard_position_por_pagina(chart_por_gold: dict[str, int]) -> str:
    """Layout em abas, uma por página do Boletim de Conjuntura.

    Hierarquia exigida pelo frontend: ROOT → TABS → TAB → ROW → CHART. Ligar
    charts direto ao ROOT, ou omitir ``meta`` em qualquer nó, produz o erro
    ``n.meta is undefined`` e o dashboard não renderiza.

    Golds sem chart correspondente são ignorados em silêncio de propósito: o
    bootstrap roda antes de `--with-charts` na primeira instalação, e uma aba
    vazia é preferível a uma exceção no meio do provisionamento.
    """
    tabs_id = "TABS-BOLETIM"
    layout: dict[str, dict] = {
        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": [tabs_id],
            "parents": [],
            "meta": {},
        },
        tabs_id: {
            "id": tabs_id,
            "type": "TABS",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
    }

    for indice_pagina, (titulo, golds) in enumerate(PAGINAS_BOLETIM):
        tab_id = f"TAB-{indice_pagina:02d}"
        layout[tabs_id]["children"].append(tab_id)
        layout[tab_id] = {
            "id": tab_id,
            "type": "TAB",
            "children": [],
            "parents": ["ROOT_ID", tabs_id],
            "meta": {"text": titulo, "defaultText": titulo, "placeholder": titulo},
        }
        for indice_gold, gold in enumerate(golds):
            chart_id = chart_por_gold.get(gold)
            if chart_id is None:
                continue
            row_id = f"ROW-{indice_pagina:02d}-{indice_gold:02d}"
            node_id = f"CHART-{chart_id}"
            layout[tab_id]["children"].append(row_id)
            layout[row_id] = {
                "id": row_id,
                "type": "ROW",
                "children": [node_id],
                "parents": ["ROOT_ID", tabs_id, tab_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[node_id] = {
                "id": node_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", tabs_id, tab_id, row_id],
                "meta": {
                    "chartId": chart_id,
                    "width": 12,
                    "height": 50,
                    "sliceName": nome_do_chart(gold),
                    "index": f"{indice_gold:03d}",
                },
            }
    return json.dumps(layout)


def nome_do_chart(gold: str) -> str:
    return f"Conjuntura | {gold.removeprefix('gld_').replace('_', ' ').title()}"


def get_or_create_dashboards(api: Superset, chart_ids: list[int]) -> None:
    existing = {item.get("slug"): item["id"] for item in api.list("dashboard")}
    for dashboard in DASHBOARDS:
        payload = {
            "dashboard_title": dashboard.title,
            "slug": dashboard.slug,
            "published": True,
            "json_metadata": json.dumps(
                {
                    "default_filters": json.dumps({"time_range": dashboard.time_range}),
                }
            ),
        }
        # Só mexe no layout quando há charts para dispor. Sem `--with-charts`,
        # `chart_ids` vem vazio e `dashboard_position([])` gera um ROOT→GRID
        # sem filhos — gravar isso APAGARIA o layout de um dashboard que já
        # está montado. O layout é conteúdo do dashboard, não do bootstrap.
        if chart_ids:
            payload["position_json"] = dashboard_position(chart_ids)

        encontrado = existing.get(dashboard.slug) or api.dashboard_id(dashboard.slug)
        if encontrado:
            if api.dry_run:
                alvo = "layout e metadados" if chart_ids else "metadados"
                print(f"[dry-run] atualizaria dashboard ({alvo}): {dashboard.slug}")
            else:
                api.update("dashboard", encontrado, payload)
                print(f"Dashboard atualizado: {dashboard.slug}")
        else:
            api.create("dashboard", payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--with-charts", action="store_true", help="Cria os 25 charts-tabulares iniciais."
    )
    args = parser.parse_args()
    load_dotenv(".env", override=False)
    api = Superset(
        env("SUPERSET_URL", "SUPERSET_HOST_PORT"),
        env("SUPERSET_USERNAME"),
        env("SUPERSET_PASSWORD"),
        args.dry_run,
    )
    database_id = get_or_create_database(api)
    datasets = get_or_create_datasets(api, database_id)
    chart_ids = get_or_create_charts(api, datasets) if args.with_charts else []
    get_or_create_dashboards(api, chart_ids)
    print(f"Concluído: {len(datasets)} datasets e {len(DASHBOARDS)} dashboards.")


if __name__ == "__main__":
    main()
