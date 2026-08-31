#!/usr/bin/env python3
"""Sincroniza a projeção semântica segura do dbt com OpenMetadata.

Não envia manifest dbt bruto: esse artefato contém SQL compilado e referências
de camadas restritas. A entrada é exclusivamente o catálogo já filtrado por
``exportar_catalogo_openmetadata.py``. Por padrão, o comando apenas materializa
o payload local; ``--confirmar`` é necessário para escrever no OpenMetadata.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CATALOG = ROOT / "docs-conjuntura" / "openmetadata_semantic_catalog.json"
DEFAULT_PAYLOAD = ROOT / "docs-conjuntura" / "openmetadata_sync_payload.json"


def api_base(url: str) -> str:
    base = url.rstrip("/")
    if base.endswith("/api/v1"):
        return base
    if base.endswith("/api"):
        return f"{base}/v1"
    return f"{base}/api/v1"


def om_data_type(value: str | None) -> str:
    data_type = (value or "").lower()
    if "timestamp" in data_type:
        return "TIMESTAMP"
    if data_type == "date":
        return "DATE"
    if "bool" in data_type:
        return "BOOLEAN"
    if "bigint" in data_type:
        return "BIGINT"
    if "int" in data_type:
        return "INT"
    if any(token in data_type for token in ("numeric", "decimal", "double", "real")):
        return "DOUBLE"
    if "json" in data_type:
        return "JSON"
    return "VARCHAR"


def om_column(column: dict) -> dict:
    """Converte um tipo SQL para a representação mínima exigida pelo OM."""
    result = {
        "name": column["name"],
        "dataType": om_data_type(column.get("data_type")),
        "description": column["description"],
    }
    # A API da instância requer comprimento explícito para VARCHAR, mesmo
    # quando a origem PostgreSQL usa TEXT (sem limite declarado).
    if result["dataType"] == "VARCHAR":
        result["dataLength"] = 65535
    return result


def schema_description(schema: str) -> str:
    """Descrição determinística de schema, sem consultar nem expor dados."""
    normalized = schema.lower()
    if normalized.endswith("_silver") or "_silver_" in normalized:
        layer = "camada Silver, com dados tratados e contratos de qualidade"
    elif normalized.endswith("_mart") or "_gold" in normalized or "_gold_" in normalized:
        layer = "camada Gold, com métricas e visões prontas para consumo analítico"
    else:
        layer = "camada publicável do produto de dados"
    return f"Schema {schema}: {layer}."


def build_payload(catalog: dict, service: str, database: str) -> dict:
    models = catalog.get("models", [])
    by_id = {model["id"]: model for model in models}
    tables = []
    schemas = {}
    ids_to_fqn = {}
    for model in models:
        fqn = f"{service}.{database}.{model['schema']}.{model['name']}"
        ids_to_fqn[model["id"]] = fqn
        schemas.setdefault(
            model["schema"],
            {
                "name": model["schema"],
                "database": f"{service}.{database}",
                "description": schema_description(model["schema"]),
            },
        )
        table = {
            "name": model["name"],
            "databaseSchema": f"{service}.{database}.{model['schema']}",
            "description": model["description"],
            "tableType": "Regular",
            "columns": [om_column(column) for column in model["columns"]],
        }
        tables.append(table)

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


def required_environment() -> tuple[str, str, str, str]:
    names = (
        "OPENMETADATA_URL",
        "OPENMETADATA_JWT_TOKEN",
        "OPENMETADATA_DATABASE_SERVICE",
        "OPENMETADATA_DATABASE_NAME",
    )
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise RuntimeError("Variáveis OpenMetadata ausentes: " + ", ".join(missing))
    return tuple(os.environ[name] for name in names)  # type: ignore[return-value]


def request(session: requests.Session, method: str, url: str, **kwargs) -> dict:
    response = session.request(method, url, timeout=30, **kwargs)
    body = response.json() if response.content else {}
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
        raise RuntimeError(
            "OpenMetadata recusou a operação: "
            + "; ".join(messages[:3])
        )
    return body


def sync(payload: dict, url: str, token: str, owner_fqn: str) -> None:
    base = api_base(url)
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    owner_response = request(
        session,
        "GET",
        f"{base}/users/name/{quote(owner_fqn, safe='')}",
    )
    owner = {
        "id": owner_response["id"],
        "type": "user",
        "name": owner_response["name"],
        "fullyQualifiedName": owner_response["fullyQualifiedName"],
    }
    request(session, "PUT", f"{base}/databaseSchemas/bulk", json=payload["database_schemas"])
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
    for table in payload["tables"]:
        fqn = f"{table['databaseSchema']}.{table['name']}"
        entity = request(session, "GET", f"{base}/tables/name/{quote(fqn, safe='')}")
        entities[fqn] = entity["id"]
    for entity_id in entities.values():
        request(
            session,
            "PATCH",
            f"{base}/tables/{entity_id}",
            headers={"Content-Type": "application/json-patch+json"},
            json=[{"op": "replace", "path": "/owners", "value": [owner]}],
        )
    for edge in payload["lineage"]:
        request(
            session,
            "PUT",
            f"{base}/lineage",
            json={
                "edge": {
                    "fromEntity": {"id": entities[edge["from_fqn"]], "type": "table"},
                    "toEntity": {"id": entities[edge["to_fqn"]], "type": "table"},
                    "description": "Dependência semântica declarada pelo dbt.",
                }
            },
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output", type=Path, default=DEFAULT_PAYLOAD)
    parser.add_argument("--service")
    parser.add_argument("--database")
    parser.add_argument("--confirmar", action="store_true")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env", override=False)
    service = args.service or os.getenv("OPENMETADATA_DATABASE_SERVICE")
    database = args.database or os.getenv("OPENMETADATA_DATABASE_NAME")
    if not service or not database:
        raise RuntimeError("Informe OPENMETADATA_DATABASE_SERVICE e OPENMETADATA_DATABASE_NAME.")
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    payload = build_payload(catalog, service, database)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        "Payload seguro: "
        f"{len(payload['database_schemas'])} schemas, "
        f"{len(payload['tables'])} tabelas e {len(payload['lineage'])} relações."
    )
    if not args.confirmar:
        print("Dry-run concluído. Use --confirmar para sincronizar com OpenMetadata.")
        return 0
    url, token, _, _ = required_environment()
    sync(payload, url, token, os.getenv("OPENMETADATA_OWNER_FQN", "admin"))
    print("Catálogo e linhagem semântica sincronizados com OpenMetadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
