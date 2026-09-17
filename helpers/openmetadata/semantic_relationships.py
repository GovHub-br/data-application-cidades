"""Catálogo e sincronização das relações semânticas do MCID.

As relações publicadas aqui são auxiliares de descoberta para RAG/GraphRAG.
Elas não criam constraints no PostgreSQL e não substituem a linhagem do dbt.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable

import yaml

RELATION_SECTION_LABELS = {
    "tested_relationships": "Contrato validado por teste dbt",
    "validated_observed": "Relação validada por agregados",
    "candidate_observed": "Relação candidata",
}


def _plain_value(value: Any) -> Any:
    """Converte wrappers Pydantic/UUID do SDK em valores comparáveis."""
    if hasattr(value, "model_dump"):
        return _plain_value(value.model_dump(mode="json", exclude_none=True))
    if hasattr(value, "root"):
        return _plain_value(value.root)
    if isinstance(value, dict):
        return {str(key): _plain_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_value(item) for item in value]
    return str(value) if value.__class__.__name__ == "UUID" else value


def _split_column_reference(reference: str) -> tuple[str, str, str]:
    parts = str(reference).split(".")
    if len(parts) != 3 or any(not part.strip() for part in parts):
        raise ValueError(
            "Referência de coluna inválida. Use schema.tabela.coluna: " f"{reference!r}"
        )
    return tuple(part.strip() for part in parts)  # type: ignore[return-value]


def _split_model_reference(reference: str) -> tuple[str, str]:
    parts = str(reference).split(".")
    if len(parts) != 2 or any(not part.strip() for part in parts):
        raise ValueError(
            f"Referência de modelo inválida. Use schema.tabela: {reference!r}"
        )
    return parts[0].strip(), parts[1].strip()


def _scope_table_keys(catalog: dict[str, Any]) -> list[str]:
    tables_by_schema = catalog["scope"]["tables_by_schema"]
    return [
        f"{schema_name}.{table_name}"
        for schema_name, table_names in tables_by_schema.items()
        for table_name in table_names
    ]


def _iter_relationships(
    catalog: dict[str, Any],
) -> Iterable[tuple[str, str, dict[str, Any]]]:
    relationships = catalog.get("relationships", {})
    for section_name in RELATION_SECTION_LABELS:
        for relationship in relationships.get(section_name, []):
            yield section_name, RELATION_SECTION_LABELS[section_name], relationship


def load_semantic_catalog(catalog_path: str) -> dict[str, Any]:
    """Carrega e valida a estrutura autocontida do catálogo."""
    path = Path(catalog_path)
    if not path.is_file():
        raise FileNotFoundError(f"Catálogo semântico não encontrado: {path}")

    catalog = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(catalog, dict):
        raise ValueError("O catálogo semântico deve ser um objeto YAML.")
    if catalog.get("version") != 1:
        raise ValueError("Apenas a versão 1 do catálogo semântico é suportada.")
    if catalog.get("kind") != "MCIDSemanticRelationshipCatalog":
        raise ValueError("kind inválido para o catálogo semântico do MCID.")

    scope = catalog.get("scope")
    if not isinstance(scope, dict):
        raise ValueError("scope é obrigatório no catálogo semântico.")
    for required_field in ("service", "database", "tables_by_schema"):
        if not scope.get(required_field):
            raise ValueError(f"scope.{required_field} é obrigatório.")

    table_keys = _scope_table_keys(catalog)
    table_key_set = set(table_keys)
    if len(table_keys) != len(table_key_set):
        raise ValueError("Há tabelas duplicadas em scope.tables_by_schema.")
    if len(table_keys) != int(scope["table_count"]):
        raise ValueError("scope.table_count diverge da quantidade em tables_by_schema.")

    relationship_ids: set[str] = set()
    for _, _, relationship in _iter_relationships(catalog):
        relationship_id = str(relationship.get("id", "")).strip()
        if not relationship_id or relationship_id in relationship_ids:
            raise ValueError(
                f"ID de relacionamento ausente ou duplicado: {relationship_id!r}"
            )
        relationship_ids.add(relationship_id)
        for endpoint_name in ("source", "target"):
            schema_name, table_name, _ = _split_column_reference(
                relationship[endpoint_name]
            )
            if f"{schema_name}.{table_name}" not in table_key_set:
                raise ValueError(
                    f"{relationship_id}: {endpoint_name} está fora do escopo."
                )

    search_group_ids: set[str] = set()
    for group in catalog.get("search_groups", []):
        group_id = str(group.get("id", "")).strip()
        if not group_id or group_id in search_group_ids:
            raise ValueError(
                f"ID de grupo de pesquisa ausente ou duplicado: {group_id!r}"
            )
        search_group_ids.add(group_id)
        members = group.get("members", [])
        if members and int(group["member_count"]) != len(members):
            raise ValueError(f"{group_id}: member_count diverge de members.")
        for member in members:
            schema_name, table_name, _ = _split_column_reference(member)
            if f"{schema_name}.{table_name}" not in table_key_set:
                raise ValueError(f"{group_id}: membro fora do escopo: {member}")
        counts = group.get("members_by_schema")
        if counts and sum(int(value) for value in counts.values()) != int(
            group["member_count"]
        ):
            raise ValueError(f"{group_id}: member_count diverge de members_by_schema.")

    join_logic = catalog.get("model_join_logic", {})
    models = join_logic.get("models", [])
    if int(join_logic.get("model_count", -1)) != len(models):
        raise ValueError("model_join_logic.model_count está inconsistente.")
    if int(join_logic.get("join_count", -1)) != sum(
        int(model["join_count"]) for model in models
    ):
        raise ValueError("model_join_logic.join_count está inconsistente.")
    for model in models:
        schema_name, table_name = _split_model_reference(model["model"])
        if f"{schema_name}.{table_name}" not in table_key_set:
            raise ValueError(f"Lógica de JOIN fora do escopo: {model['model']}")
        if int(model["join_count"]) != len(model.get("predicates", [])):
            raise ValueError(f"join_count diverge dos predicados em {model['model']}.")

    properties = catalog.get("openmetadata", {}).get("custom_properties", {})
    for property_name in ("markdown", "related_tables"):
        if not properties.get(property_name, {}).get("name"):
            raise ValueError(
                f"openmetadata.custom_properties.{property_name}.name é obrigatório."
            )

    return catalog


def validate_catalog_against_dbt(
    catalog_path: str,
    models_dir: str,
) -> dict[str, int]:
    """Confere tabelas e colunas do catálogo contra os schema.yml do dbt."""
    catalog = load_semantic_catalog(catalog_path)
    models_path = Path(models_dir)
    model_columns: dict[str, set[str]] = {}

    for schema_path in sorted(models_path.glob("**/schema.yml")):
        schema = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
        for model in (schema or {}).get("models", []):
            name = str(model["name"])
            if name in model_columns:
                raise ValueError(f"Modelo dbt duplicado no inventário: {name}")
            model_columns[name] = {
                str(column["name"]) for column in model.get("columns", [])
            }

    table_keys = _scope_table_keys(catalog)
    for table_key in table_keys:
        _, table_name = _split_model_reference(table_key)
        if table_name not in model_columns:
            raise ValueError(f"Tabela do catálogo sem modelo dbt: {table_key}")

    def validate_column(reference: str) -> None:
        _, table_name, column_name = _split_column_reference(reference)
        if column_name not in model_columns[table_name]:
            raise ValueError(f"Coluna do catálogo ausente no dbt: {reference}")

    for _, _, relationship in _iter_relationships(catalog):
        validate_column(relationship["source"])
        validate_column(relationship["target"])
    for group in catalog.get("search_groups", []):
        for member in group.get("members", []):
            validate_column(member)
    for model in catalog.get("model_join_logic", {}).get("models", []):
        for reference in model.get("upstream_columns", []):
            validate_column(reference)

    scoped_model_names = {
        _split_model_reference(table_key)[1] for table_key in table_keys
    }
    column_count = sum(
        len(model_columns[model_name]) for model_name in scoped_model_names
    )
    if column_count != int(catalog["scope"]["column_count"]):
        raise ValueError("scope.column_count diverge das colunas documentadas no dbt.")

    return {
        "tables": len(table_keys),
        "columns": column_count,
        "relationships": sum(1 for _ in _iter_relationships(catalog)),
        "searchGroups": len(catalog.get("search_groups", [])),
        "joinModels": len(catalog["model_join_logic"]["models"]),
        "joinClauses": int(catalog["model_join_logic"]["join_count"]),
    }


def _table_fqn(catalog: dict[str, Any], table_key: str) -> str:
    schema_name, table_name = _split_model_reference(table_key)
    scope = catalog["scope"]
    return ".".join((scope["service"], scope["database"], schema_name, table_name))


def _search_groups_for_table(
    catalog: dict[str, Any],
    table_key: str,
) -> list[tuple[dict[str, Any], list[str]]]:
    groups = []
    for group in catalog.get("search_groups", []):
        columns = []
        for reference in group.get("members", []):
            schema_name, table_name, column_name = _split_column_reference(reference)
            if f"{schema_name}.{table_name}" == table_key:
                columns.append(column_name)
        if columns:
            groups.append((group, sorted(set(columns))))
    return groups


def _relationships_for_table(
    catalog: dict[str, Any],
    table_key: str,
) -> list[tuple[str, dict[str, Any], str, str]]:
    result = []
    for section_name, section_label, relationship in _iter_relationships(catalog):
        source = ".".join(_split_column_reference(relationship["source"])[:2])
        target = ".".join(_split_column_reference(relationship["target"])[:2])
        if table_key == source:
            result.append((section_label, relationship, "origem", relationship["target"]))
        elif table_key == target:
            result.append(
                (section_label, relationship, "destino", relationship["source"])
            )
    return result


def _join_logic_for_table(
    catalog: dict[str, Any],
    table_key: str,
) -> dict[str, Any] | None:
    return next(
        (
            item
            for item in catalog.get("model_join_logic", {}).get("models", [])
            if item["model"] == table_key
        ),
        None,
    )


def _coverage_text(relationship: dict[str, Any]) -> str:
    evidence = relationship.get("evidence")
    if not isinstance(evidence, dict):
        return ""
    common = evidence.get("common_distinct_keys")
    source = evidence.get("source", {})
    target = evidence.get("target", {})
    details = []
    if common is not None:
        details.append(f"{common} chaves distintas em comum")
    if source.get("distinct_coverage") is not None:
        details.append(
            f"cobertura origem {100 * float(source['distinct_coverage']):.1f}%"
        )
    if target.get("distinct_coverage") is not None:
        details.append(
            f"cobertura destino {100 * float(target['distinct_coverage']):.1f}%"
        )
    if evidence.get("source_coverage") is not None:
        details.append(
            f"cobertura origem {100 * float(evidence['source_coverage']):.1f}%"
        )
    if evidence.get("target_coverage") is not None:
        details.append(
            f"cobertura destino {100 * float(evidence['target_coverage']):.1f}%"
        )
    return "; ".join(details)


def render_table_markdown(
    catalog: dict[str, Any],
    table_key: str,
) -> str:
    """Renderiza o conteúdo humano que aparece na propriedade da tabela."""
    if table_key not in set(_scope_table_keys(catalog)):
        raise ValueError(f"Tabela fora do escopo: {table_key}")

    relationships = _relationships_for_table(catalog, table_key)
    search_groups = _search_groups_for_table(catalog, table_key)
    join_logic = _join_logic_for_table(catalog, table_key)
    catalog_id = catalog["metadata"]["catalog_id"]
    snapshot = catalog["metadata"]["profile_generated_at"]

    lines = [
        "## Relações semânticas MCID",
        "",
        (
            f"Catálogo `{catalog_id}`, evidência agregada de `{snapshot}`. "
            "Estas anotações apoiam descoberta/RAG; não são constraints FK. "
            "A linhagem de transformação permanece no grafo do dbt."
        ),
        "",
        "### Chaves e campos de pesquisa",
        "",
    ]
    if search_groups:
        for group, columns in search_groups:
            uses = ", ".join(group.get("use", []))
            lines.append(
                f"- **{group['id']}**: "
                + ", ".join(f"`{column}`" for column in columns)
                + f". Uso: {uses}. Normalização: "
                + str(group.get("normalization", group.get("match", "documentada")))
                + f". Privacidade: {group.get('pii', 'revisar metadados')}."
            )
            for guardrail in group.get("guardrails", []):
                lines.append(f"  - Cautela: {guardrail}")
    else:
        lines.append(
            "- Nenhuma chave/grupo explícito foi atribuído a esta tabela. "
            "Use as descrições dbt e os grupos globais de datas/códigos somente "
            "com revisão semântica."
        )

    lines.extend(["", "### Relações explícitas", ""])
    if relationships:
        for section_label, relationship, role, other_endpoint in relationships:
            coverage = _coverage_text(relationship)
            detail = (
                f"- **{section_label}** ({role}): "
                f"`{relationship['source']}` → `{relationship['target']}`; "
                f"chave `{relationship['semantic_key']}`; "
                f"JOIN `{relationship['join']}`; "
                f"cardinalidade `{relationship['cardinality']}`; "
                f"confiança `{relationship['confidence']}`"
            )
            if coverage:
                detail += f"; {coverage}"
            detail += "."
            lines.append(detail)
            if relationship.get("notes"):
                lines.append(f"  - Nota: {relationship['notes']}")
            if other_endpoint:
                lines.append(f"  - Outra ponta: `{other_endpoint}`.")
    else:
        lines.append(
            "- Nenhuma relação explícita do catálogo toca esta tabela; consulte "
            "a aba de linhagem para dependências de transformação."
        )

    lines.extend(["", "### Lógica de JOIN do modelo", ""])
    if join_logic:
        for predicate in join_logic["predicates"]:
            lines.append(f"- `{predicate}`")
        if join_logic.get("guardrail"):
            lines.append(f"- Cautela: {join_logic['guardrail']}")
    else:
        lines.append(
            "- O SQL deste modelo não contém JOIN próprio inventariado. "
            "Dependências por `ref()`/`source()` continuam disponíveis na linhagem."
        )

    lines.extend(
        [
            "",
            "### Privacidade e interpretação",
            "",
            (
                "- CNPJ, nomes/razões sociais, endereço e geolocalização exigem "
                "controle de acesso. Não envie valores brutos a embeddings; use "
                "token exato protegido, máscara ou granularidade espacial adequada."
            ),
            (
                "- Relações candidatas e sobreposição parcial precisam de "
                "corroboração antes de fundir nós no GraphRAG."
            ),
        ]
    )
    return "\n".join(lines)


def _related_table_keys(
    catalog: dict[str, Any],
    table_key: str,
) -> list[str]:
    related = set()
    for _, _, relationship in _iter_relationships(catalog):
        source = ".".join(_split_column_reference(relationship["source"])[:2])
        target = ".".join(_split_column_reference(relationship["target"])[:2])
        if source == table_key and target != table_key:
            related.add(target)
        if target == table_key and source != table_key:
            related.add(source)
    return sorted(related)


def _normalize_sdk_host(host_port: str) -> str:
    host = str(host_port).strip().rstrip("/")
    if not host:
        raise ValueError("OM_HOST não pode ser vazio.")
    if host.endswith("/api/v1"):
        return host[:-3]
    if host.endswith("/api"):
        return host
    return f"{host}/api"


def _extension_dict(table: Any) -> dict[str, Any]:
    extension = _plain_value(getattr(table, "extension", None))
    return extension if isinstance(extension, dict) else {}


def _reference_payload(table: Any) -> dict[str, str]:
    return {
        "id": str(_plain_value(table.id)),
        "type": "table",
        "name": str(_plain_value(table.name)),
        "fullyQualifiedName": str(_plain_value(table.fullyQualifiedName)),
    }


def _reference_identity_set(value: Any) -> set[tuple[str, str]]:
    plain = _plain_value(value)
    if isinstance(plain, dict) and "root" in plain:
        plain = plain["root"]
    if not isinstance(plain, list):
        return set()
    result = set()
    for item in plain:
        if isinstance(item, dict):
            result.add(
                (
                    str(item.get("id", "")),
                    str(item.get("fullyQualifiedName", "")),
                )
            )
    return result


def _json_pointer_segment(value: str) -> str:
    """Escapa um nome para uso seguro em um caminho JSON Pointer."""
    return str(value).replace("~", "~0").replace("/", "~1")


def _patch_custom_properties_preserving_extension(
    metadata: Any,
    entity: Any,
    table: Any,
    custom_properties: dict[str, Any],
) -> Any:
    """Atualiza apenas as propriedades informadas sem substituir /extension.

    O helper ``patch_custom_properties`` do SDK 1.12.1 refaz a leitura da
    entidade sem solicitar ``extension``. Isso pode fazê-lo enxergar um objeto
    vazio e sobrescrever propriedades de terceiros. Aqui usamos o ``extension``
    já carregado no preflight e aplicamos JSON Patch por propriedade.
    """
    existing = _extension_dict(table)
    if existing:
        operations = [
            {
                "op": "replace" if name in existing else "add",
                "path": f"/extension/{_json_pointer_segment(name)}",
                "value": _plain_value(value),
            }
            for name, value in custom_properties.items()
        ]
    else:
        operations = [
            {
                "op": "add",
                "path": "/extension",
                "value": _plain_value(custom_properties),
            }
        ]

    response = metadata.client.patch(
        path=(f"{metadata.get_suffix(entity)}/" f"{str(_plain_value(table.id))}"),
        data=json.dumps(operations, ensure_ascii=False),
    )
    if not response:
        raise RuntimeError("OpenMetadata não retornou a tabela após o PATCH semântico.")
    return response


def sync_semantic_relationships(
    catalog_path: str,
    host_port: str,
    jwt_token: str,
    dry_run: bool = False,
) -> dict[str, int | str]:
    """Cria propriedades de Table e publica o catálogo de forma idempotente."""
    catalog = load_semantic_catalog(catalog_path)
    table_keys = _scope_table_keys(catalog)
    summary: dict[str, int | str] = {
        "catalog": catalog["metadata"]["catalog_id"],
        "tables": len(table_keys),
        "relationships": sum(1 for _ in _iter_relationships(catalog)),
        "searchGroups": len(catalog.get("search_groups", [])),
        "joinClauses": int(catalog["model_join_logic"]["join_count"]),
        "patched": 0,
        "unchanged": 0,
    }
    if dry_run:
        logging.info("Catálogo semântico validado em dry-run: %s", summary)
        return summary

    jwt_token = str(jwt_token).strip()
    if not jwt_token:
        raise ValueError("INGESTION_TOKEN não pode ser vazio.")

    # Imports tardios: o módulo também é testado fora do virtualenv isolado.
    from metadata.generated.schema.api.data.createCustomProperty import (
        CreateCustomPropertyRequest,
    )
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E501
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.generated.schema.type.customProperty import (
        CustomPropertyConfig,
        EntityTypes,
    )
    from metadata.ingestion.models.custom_properties import (
        CustomPropertyDataTypes,
        OMetaCustomProperties,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    metadata = OpenMetadata(
        OpenMetadataConnection(
            hostPort=_normalize_sdk_host(host_port),
            authProvider="openmetadata",
            securityConfig=OpenMetadataJWTClientConfig(jwtToken=jwt_token),
        )
    )
    property_config = catalog["openmetadata"]["custom_properties"]

    markdown_definition = property_config["markdown"]
    metadata.create_or_update_custom_property(
        OMetaCustomProperties(
            entity_type=Table,
            createCustomPropertyRequest=CreateCustomPropertyRequest(
                name=markdown_definition["name"],
                displayName=markdown_definition["display_name"],
                description=markdown_definition["description"],
                propertyType=metadata.get_property_type_ref(
                    CustomPropertyDataTypes.MARKDOWN
                ),
            ),
        )
    )

    related_definition = property_config["related_tables"]
    metadata.create_or_update_custom_property(
        OMetaCustomProperties(
            entity_type=Table,
            createCustomPropertyRequest=CreateCustomPropertyRequest(
                name=related_definition["name"],
                displayName=related_definition["display_name"],
                description=related_definition["description"],
                propertyType=metadata.get_property_type_ref(
                    CustomPropertyDataTypes.ENTITY_REFERENCE_LIST
                ),
                customPropertyConfig=CustomPropertyConfig(
                    config=EntityTypes(root=["table"])
                ),
            ),
        )
    )

    tables: dict[str, Any] = {}
    missing_tables = []
    for table_key in table_keys:
        fqn = _table_fqn(catalog, table_key)
        table = metadata.get_by_name(
            entity=Table,
            fqn=fqn,
            fields=["columns", "extension"],
            nullable=True,
        )
        if table is None:
            missing_tables.append(fqn)
        else:
            tables[table_key] = table
    if missing_tables:
        raise RuntimeError(
            "Tabelas do catálogo ausentes no OpenMetadata: " + ", ".join(missing_tables)
        )

    # Evita publicar referências que ficaram obsoletas após alteração de schema.
    live_columns = {
        table_key: {
            str(_plain_value(column.name))
            for column in (getattr(table, "columns", None) or [])
        }
        for table_key, table in tables.items()
    }
    for _, _, relationship in _iter_relationships(catalog):
        for endpoint_name in ("source", "target"):
            schema_name, table_name, column_name = _split_column_reference(
                relationship[endpoint_name]
            )
            table_key = f"{schema_name}.{table_name}"
            if column_name not in live_columns[table_key]:
                raise RuntimeError(
                    "Coluna do catálogo ausente no OpenMetadata: "
                    f"{relationship[endpoint_name]}"
                )

    markdown_name = markdown_definition["name"]
    related_name = related_definition["name"]
    for table_key, table in tables.items():
        markdown = render_table_markdown(catalog, table_key)
        related_payload = [
            _reference_payload(tables[related_key])
            for related_key in _related_table_keys(catalog, table_key)
        ]
        existing = _extension_dict(table)
        unchanged = str(
            existing.get(markdown_name, "")
        ) == markdown and _reference_identity_set(
            existing.get(related_name)
        ) == _reference_identity_set(
            related_payload
        )
        if unchanged:
            summary["unchanged"] = int(summary["unchanged"]) + 1
            continue

        # a chamada existe pelo efeito; o retorno não é usado
        _patch_custom_properties_preserving_extension(
            metadata=metadata,
            entity=Table,
            table=table,
            custom_properties={
                markdown_name: markdown,
                related_name: related_payload,
            },
        )
        summary["patched"] = int(summary["patched"]) + 1

    # Garante que o resumo nunca carregue objetos do SDK ou segredos.
    logging.info(
        "Catálogo semântico sincronizado: %s",
        json.dumps(summary, ensure_ascii=False, sort_keys=True),
    )
    return summary
