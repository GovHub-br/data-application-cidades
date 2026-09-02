#!/usr/bin/env python3
"""Sincroniza a estrutura do catálogo — schema, tabela, coluna e linhagem.

Não envia manifest dbt bruto: esse artefato contém SQL compilado e referências
de camadas restritas. A entrada é exclusivamente o catálogo já filtrado por
``exportar_catalogo_openmetadata.py``. Por padrão, o comando apenas materializa
o payload local; ``--confirmar`` é necessário para escrever no OpenMetadata.

Este script cuida só da ESTRUTURA. Domínio, produto de dados, proprietário,
etiqueta, certificação e glossário são governança e ficam em
``sincronizar_governanca.py`` — antes os dois mexiam em `/owners` e o segundo a
rodar desfazia o primeiro.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from governanca_comum import RAIZ, ambiente, carregar, mesmo_texto
from linhagem_colunas import agrupar_por_aresta, derivar, schema_do_catalogo
from restricoes_dbt import (
    Restricoes,
    carregar as carregar_restricoes,
    constraints_da_tabela,
)

DEFAULT_CATALOG = RAIZ / "dbt" / "mcid" / "governance" / "openmetadata_semantic_catalog.json"
DEFAULT_PAYLOAD = RAIZ / "build" / "openmetadata_sync_payload.json"

#: Tipos do PostgreSQL para o vocabulário do OpenMetadata. O mapa é explícito
#: de propósito: a versão anterior colapsava tudo em sete tipos e carimbava
#: `VARCHAR(65535)` em cada coluna textual — 868 colunas descritas com um
#: comprimento que nenhuma delas tem.
TIPOS = {
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "double precision": "DOUBLE",
    "real": "FLOAT",
    "text": "TEXT",
    "uuid": "UUID",
    "bytea": "BYTES",
    "json": "JSON",
    "jsonb": "JSON",
    "interval": "INTERVAL",
    "time without time zone": "TIME",
    "time with time zone": "TIME",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPZ",
}
#: `character varying` sem limite não tem comprimento a declarar. Publicar como
#: TEXT é fiel; publicar como VARCHAR obrigaria a inventar um tamanho.
TIPOS_TEXTO_SEM_LIMITE = {"character varying", "varchar", "character", "bpchar"}
PARAMETROS = re.compile(r"^([a-z ]+?)\s*\((\d+)(?:\s*,\s*(\d+))?\)$")


def api_base(url: str) -> str:
    base = url.rstrip("/")
    if base.endswith("/api/v1"):
        return base
    if base.endswith("/api"):
        return f"{base}/v1"
    return f"{base}/api/v1"


def om_column(column: dict, restricoes: Restricoes | None = None) -> dict:
    """Traduz a coluna preservando tipo, precisão, comprimento e posição."""
    bruto = (column.get("data_type") or "").strip().lower()
    resultado: dict = {"name": column["name"], "description": column["description"]}

    if bruto.endswith("[]"):
        interno = om_column({**column, "data_type": bruto[:-2]})
        resultado["dataType"] = "ARRAY"
        resultado["arrayDataType"] = interno["dataType"]
        resultado["dataTypeDisplay"] = bruto
    else:
        casado = PARAMETROS.match(bruto)
        base = casado.group(1).strip() if casado else bruto
        primeiro = int(casado.group(2)) if casado else None
        segundo = int(casado.group(3)) if casado and casado.group(3) else None

        if base in ("numeric", "decimal"):
            resultado["dataType"] = "NUMERIC"
            if primeiro is not None:
                resultado["precision"] = primeiro
                resultado["scale"] = segundo or 0
        elif base in TIPOS_TEXTO_SEM_LIMITE:
            if primeiro is not None:
                resultado["dataType"] = "VARCHAR"
                resultado["dataLength"] = primeiro
            else:
                resultado["dataType"] = "TEXT"
        else:
            resultado["dataType"] = TIPOS.get(base, "TEXT")
        if bruto:
            resultado["dataTypeDisplay"] = bruto

    if column.get("ordinal") is not None:
        resultado["ordinalPosition"] = column["ordinal"]
    # O dbt não tem `primary key`, tem teste: `unique` + `not_null` é a chave.
    restricao = restricoes.restricao_da_coluna(column["name"]) if restricoes else None
    if restricao:
        resultado["constraint"] = restricao
    return resultado


#: Materialização do dbt para o tipo de entidade do OpenMetadata. Antes toda
#: entidade era `Regular`, inclusive as views.
TIPOS_DE_TABELA = {
    "table": "Regular",
    "incremental": "Regular",
    "view": "View",
    "materialized_view": "MaterializedView",
    "ephemeral": "Regular",
}


def build_payload(catalog: dict, service: str, database: str) -> dict:
    """Monta o payload estrutural a partir do catálogo semântico filtrado."""
    declarados = {
        item["name"]: item for item in carregar("schemas.yml").get("schemas", [])
    }
    models = catalog.get("models", [])
    todas_restricoes = carregar_restricoes()
    by_id = {model["id"]: model for model in models}
    tables = []
    schemas: dict[str, dict] = {}
    ids_to_fqn = {}

    ausentes = sorted({m["schema"] for m in models} - declarados.keys())
    if ausentes:
        # Antes, um schema não declarado recebia uma frase montada por regra de
        # sufixo do nome. Descrição de catálogo é curadoria: se não foi escrita,
        # tem de faltar em voz alta.
        raise RuntimeError(
            "Schemas presentes no catálogo e ausentes de governance/schemas.yml: "
            + ", ".join(ausentes)
        )

    for model in models:
        fqn = f"{service}.{database}.{model['schema']}.{model['name']}"
        ids_to_fqn[model["id"]] = fqn
        declarado = declarados[model["schema"]]
        schemas.setdefault(
            model["schema"],
            {
                "name": model["schema"],
                "database": f"{service}.{database}",
                "description": declarado["description"],
                **(
                    {"displayName": declarado["display_name"]}
                    if declarado.get("display_name")
                    else {}
                ),
            },
        )
        restricoes = todas_restricoes.get(model["name"])
        colunas_publicadas = {coluna["name"] for coluna in model["columns"]}
        tabela = {
            "name": model["name"],
            "databaseSchema": f"{service}.{database}.{model['schema']}",
            "description": model["description"],
            "tableType": TIPOS_DE_TABELA.get(model.get("materialized"), "Regular"),
            "columns": [om_column(coluna, restricoes) for coluna in model["columns"]],
        }
        if restricoes:
            chaves = constraints_da_tabela(restricoes, colunas_publicadas)
            if chaves:
                tabela["tableConstraints"] = chaves
        tables.append(tabela)

    # A projeção só conecta entidades publicáveis. Dependências de Bronze/Raw
    # permanecem fora do OpenMetadata/RAG, mas a linhagem Silver -> Gold e
    # Silver -> Silver é preservada sem SQL nem mapeamento coluna a coluna.
    lineage = [
        {"from_fqn": ids_to_fqn[parent], "to_fqn": ids_to_fqn[model["id"]]}
        for model in models
        for parent in model.get("depends_on", [])
        if parent in by_id and parent in ids_to_fqn
    ]
    return {
        "version": 1,
        "database_schemas": list(schemas.values()),
        "tables": tables,
        "lineage": lineage,
    }


#: Reescrever o array de colunas de uma tabela larga leva bem mais que os 30
#: segundos originais — a sincronização morreu no meio por causa disso, com a
#: metade das descrições aplicada e a outra metade não.
TEMPO_LIMITE = 120
#: Timeout e queda de conexão são transitórios; falhar a carga inteira por
#: causa de um deles obriga a recomeçar do zero uma operação de 140 tabelas.
TENTATIVAS = 3


def request(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict:
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            response = session.request(method, url, timeout=TEMPO_LIMITE, **kwargs)
            break
        except (requests.Timeout, requests.ConnectionError) as exc:
            if tentativa == TENTATIVAS:
                raise RuntimeError(
                    f"OpenMetadata não respondeu em {TENTATIVAS} tentativas: {exc}"
                ) from exc
            espera = 5 * tentativa
            print(f"  rede instável ({type(exc).__name__}); nova tentativa em {espera}s")
            time.sleep(espera)
    body: dict = response.json() if response.content else {}
    if not response.ok:
        message = body.get("message") if isinstance(body, dict) else None
        raise RuntimeError(
            f"OpenMetadata respondeu HTTP {response.status_code}"
            + (f": {message}" if message else "")
        )
    if body.get("status") == "failure":
        messages = [
            str(item.get("message", "erro de validação"))
            for item in body.get("failedRequest", [])
        ]
        raise RuntimeError("OpenMetadata recusou a operação: " + "; ".join(messages[:3]))
    return body


#: Campos de coluna comparados para decidir se vale reescrever o array inteiro.
#: Campo que não está aqui NUNCA é escrito: a comparação não vê divergência e
#: o patch não sai. `ordinalPosition` ficou de fora na primeira versão e a
#: posição física das 2244 colunas simplesmente não subia.
CAMPOS_DE_COLUNA = (
    "dataType",
    "dataLength",
    "precision",
    "scale",
    "description",
    "constraint",
    "ordinalPosition",
)


def colunas_divergem(atuais: list[dict], desejadas: list[dict]) -> bool:
    """Diz se as colunas publicadas diferem do que o dbt documenta."""
    por_nome = {c["name"]: c for c in atuais}
    if set(por_nome) != {c["name"] for c in desejadas}:
        return True
    for desejada in desejadas:
        atual = por_nome[desejada["name"]]
        for campo in CAMPOS_DE_COLUNA:
            if campo == "description":
                if not mesmo_texto(atual.get(campo), desejada.get(campo)):
                    return True
            elif (atual.get(campo) or None) != (desejada.get(campo) or None):
                return True
    return False


def patch_json(session: requests.Session, url: str, operacoes: list[dict]) -> dict:
    return request(
        session,
        "PATCH",
        url,
        headers={"Content-Type": "application/json-patch+json"},
        json=operacoes,
    )


def linhagem_de_colunas(payload: dict) -> dict[tuple[str, str], list[dict]]:
    """Mapeamento coluna a coluna por aresta, em FQN completo.

    Publica só a correspondência entre colunas; o SQL é lido localmente para
    derivá-la e nunca sai daqui.
    """
    fqn_por_modelo = {
        tabela["name"]: f"{tabela['databaseSchema']}.{tabela['name']}"
        for tabela in payload["tables"]
    }
    vinculos, _ = derivar(schema_do_catalogo())
    resultado: dict[tuple[str, str], list[dict]] = {}
    for (origem, destino), colunas in agrupar_por_aresta(vinculos).items():
        if origem not in fqn_por_modelo or destino not in fqn_por_modelo:
            continue
        resultado[(fqn_por_modelo[origem], fqn_por_modelo[destino])] = [
            {
                "fromColumns": [f"{fqn_por_modelo[origem]}.{c}" for c in origens],
                "toColumn": f"{fqn_por_modelo[destino]}.{coluna}",
            }
            for origens, coluna in colunas
        ]
    return resultado


def publicar_linhagem(
    session: requests.Session, base: str, payload: dict, entities: dict[str, str]
) -> None:
    """Publica a linhagem entre tabelas, com o mapeamento de coluna embutido.

    O OpenMetadata guarda a linhagem de coluna DENTRO da aresta entre as duas
    tabelas, então ela vai junto e não numa segunda passada.
    """
    colunas_por_aresta = linhagem_de_colunas(payload)
    for edge in payload["lineage"]:
        corpo: dict = {
            "fromEntity": {"id": entities[edge["from_fqn"]], "type": "table"},
            "toEntity": {"id": entities[edge["to_fqn"]], "type": "table"},
            "description": "Dependência semântica declarada pelo dbt.",
        }
        mapeamento = colunas_por_aresta.get((edge["from_fqn"], edge["to_fqn"]))
        if mapeamento:
            corpo["lineageDetails"] = {"columnsLineage": mapeamento}
        request(session, "PUT", f"{base}/lineage", json={"edge": corpo})
    com_coluna = sum(
        1
        for edge in payload["lineage"]
        if (edge["from_fqn"], edge["to_fqn"]) in colunas_por_aresta
    )
    total = len(payload["lineage"])
    print(f"Linhagem: {total} arestas, {com_coluna} com mapeamento de coluna.")


def sync(payload: dict, url: str, token: str) -> None:
    base = api_base(url)
    session = requests.Session()
    session.headers.update(
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    request(
        session, "PUT", f"{base}/databaseSchemas/bulk", json=payload["database_schemas"]
    )
    # O `PUT` cria o que falta e atualiza o nome de exibição, mas NÃO sobrescreve
    # descrição já preenchida: a instância preserva o texto existente quando a
    # atualização vem de ingestão. Foi assim que as descrições de rodapé da
    # primeira carga sobreviveram à documentação curada do dbt — o catálogo
    # parecia documentado e trazia "Modelo da camada gold do produto conjuntura"
    # em 85 tabelas. Descrição, portanto, só entra por PATCH.
    for schema in payload["database_schemas"]:
        fqn = f"{schema['database']}.{schema['name']}"
        atual = request(
            session, "GET", f"{base}/databaseSchemas/name/{quote(fqn, safe='')}"
        )
        if not mesmo_texto(atual.get("description"), schema["description"]):
            patch_json(
                session,
                f"{base}/databaseSchemas/{atual['id']}",
                [{"op": "add", "path": "/description", "value": schema["description"]}],
            )

    # A instância aceita a rota bulk, mas rejeita mais de uma entidade por
    # requisição. O envio unitário pela rota bulk é idempotente e compatível
    # com esse comportamento, além de isolar falhas por tabela.
    for table in payload["tables"]:
        try:
            request(session, "PUT", f"{base}/tables/bulk", json=[table])
        except RuntimeError as exc:
            raise RuntimeError(
                f"Falha ao sincronizar metadados da tabela {table['name']}: {exc}"
            ) from exc

    entities = {}
    ajustadas = 0
    for table in payload["tables"]:
        fqn = f"{table['databaseSchema']}.{table['name']}"
        entity = request(
            session, "GET", f"{base}/tables/name/{quote(fqn, safe='')}?fields=columns"
        )
        entities[fqn] = entity["id"]
        operacoes = []
        if not mesmo_texto(entity.get("description"), table["description"]):
            operacoes.append(
                {"op": "add", "path": "/description", "value": table["description"]}
            )
        # Reescrever `/columns` substitui o array inteiro e leva junto a etiqueta
        # de glossário que a governança pendura na coluna. É por isso que a ordem
        # do `make openmetadata` é estrutura primeiro, governança depois.
        if colunas_divergem(entity.get("columns") or [], table["columns"]):
            operacoes.append({"op": "add", "path": "/columns", "value": table["columns"]})
        if operacoes:
            patch_json(session, f"{base}/tables/{entity['id']}", operacoes)
            ajustadas += 1
    print(f"Descrição e colunas ajustadas em {ajustadas} tabelas.")
    publicar_linhagem(session, base, payload, entities)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output", type=Path, default=DEFAULT_PAYLOAD)
    parser.add_argument("--service")
    parser.add_argument("--database")
    parser.add_argument("--confirmar", action="store_true")
    args = parser.parse_args()

    config = ambiente(exigir_acesso=args.confirmar)
    service = args.service or config["servico"]
    database = args.database or config["banco"]
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    payload = build_payload(catalog, service, database)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        "Payload seguro: "
        f"{len(payload['database_schemas'])} schemas, "
        f"{len(payload['tables'])} tabelas e {len(payload['lineage'])} relações."
    )
    if not args.confirmar:
        print("Dry-run concluído. Use --confirmar para sincronizar com OpenMetadata.")
        return 0
    sync(payload, config["url"], config["token"])
    print("Estrutura e linhagem sincronizadas com OpenMetadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
