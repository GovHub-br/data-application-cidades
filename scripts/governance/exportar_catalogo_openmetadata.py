#!/usr/bin/env python3
"""Exporta catálogo semântico seguro de artefatos privados do dbt.

O OpenMetadata e o GraphRAG devem receber significado, relação e governança;
nunca SQL compilado, caminhos de lake, amostras ou valores de dados. Este
comando lê ``manifest.json`` e ``catalog.json`` gerados em diretório privado e
só persiste o subconjunto autorizado de modelos Silver e Gold. Snapshots são
histórico operacional e nunca são publicados neste catálogo. Colunas classificadas
como sensíveis são omitidas do corpus, sem expor sequer o seu identificador.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

from semantic_descriptions import for_column

ROOT = Path(__file__).resolve().parents[2]
DBT_TARGET = ROOT / "dbt" / "mcid" / "target"
GOVERNANCE = ROOT / "dbt" / "mcid" / "governance"

SENSITIVE_IDENTIFIERS = (
    "cpf",
    "cnpj",
    "mutuario",
    "nascimento",
    "logradouro",
    "endereco",
    "telefone",
    "celular",
    "email",
    "nis",
    "titular",
    "beneficiario",
    "cep",
)
UNSAFE_DESCRIPTION_PATTERNS = {
    "exemplo": re.compile(r"\b(?:ex\.?|exemplo|por exemplo)\b", re.IGNORECASE),
    "mapeamento_literal": re.compile(r"\b\d+\s*=\s*[^\s]"),
    "caminho_tecnico": re.compile(
        r"(?:s3://|\braw/|\bstaging/|manual_conjuntura\.)", re.IGNORECASE
    ),
}
EXCLUDED_RESOURCE_TYPES = {"snapshot", "seed", "source"}


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Artefato dbt privado ausente: {path}")
    conteudo: dict = json.loads(path.read_text(encoding="utf-8"))
    return conteudo


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def is_sensitive_identifier(value: str) -> bool:
    lowered = value.lower()
    return lowered == "cep" or any(
        pattern in lowered for pattern in SENSITIVE_IDENTIFIERS
    )


def unsafe_description(value: str) -> str | None:
    if is_sensitive_identifier(value):
        return "identificador_sensivel"
    for rule, pattern in UNSAFE_DESCRIPTION_PATTERNS.items():
        if pattern.search(value):
            return rule
    return None


def fail(violations: list[str], message: str) -> None:
    violations.append(message)


#: Camadas que entram no catálogo mesmo sem serem elegíveis a RAG. Existem
#: para que a LINHAGEM alcance a origem: com a Bronze fora, o grafo começa na
#: Silver e o consumidor não vê de onde o número veio. O que se publica delas
#: é a existência e a topologia — descrição de tabela e coluna seguem as
#: mesmas regras de omissão de identificador, e o corpus de RAG continua sem
#: elas.
CAMADAS_SO_PARA_LINHAGEM = {"bronze"}


def model_export(
    node_id: str, node: dict, catalog: dict, violations: list[str]
) -> dict | None:
    governance = node.get("meta", {}).get("governance", {})
    elegivel_rag = (
        governance.get("rag_publication") == "eligible_after_security_validation"
    )
    so_linhagem = governance.get("layer") in CAMADAS_SO_PARA_LINHAGEM
    if not elegivel_rag and not so_linhagem:
        return None

    description = (node.get("description") or "").strip()
    # variável própria: antes o laço de colunas reaproveitava este nome e o
    # status publicado para a TABELA acabava sendo o da última coluna lida.
    status_da_tabela = "curated"
    if not description or unsafe_description(description):
        description = (
            f"Modelo da camada {governance.get('layer', 'analítica')} do produto "
            f"{governance.get('product', 'MCID')}."
        )
        status_da_tabela = "derived_convention"

    catalog_columns = catalog.get("nodes", {}).get(node_id, {}).get("columns", {})
    declared_columns = node.get("columns", {})
    columns = []
    for column_name, catalog_column in sorted(catalog_columns.items()):
        if is_sensitive_identifier(column_name):
            # A semântica segura do model continua recuperável pelo RAG; a
            # coluna restrita não entra no artefato publicado nem no log.
            continue
        declared = declared_columns.get(column_name, {})
        column_description = (declared.get("description") or "").strip()
        documentation_status = "curated"  # status desta coluna, não o da tabela
        # Enquanto o YAML é curado, o catálogo recebe uma descrição
        # determinística baseada somente no nome técnico. Isso impede campos
        # mudos no RAG sem vazar exemplos, mapeamentos ou conteúdo de dados.
        if not column_description or unsafe_description(column_description):
            column_description = for_column(column_name)
            documentation_status = "derived_convention"
        columns.append(
            {
                "name": column_name,
                # tipo como o banco o declara, com precisão e comprimento
                # (`numeric(15,2)`, `character varying(50)`): é o que permite
                # ao sync publicar o tipo fiel em vez de colapsar tudo em
                # VARCHAR de tamanho fixo.
                "data_type": catalog_column.get("type") or declared.get("data_type"),
                # posição física da coluna na tabela. As colunas são ordenadas
                # por nome neste arquivo (determinismo do artefato), então sem
                # este campo a ordem original se perderia.
                "ordinal": catalog_column.get("index"),
                "description": column_description,
                "documentation_status": documentation_status,
            }
        )

    if not catalog_columns:
        fail(violations, f"{node_id}: sem colunas no catalog.json privado")

    return {
        "id": node_id,
        "product": governance.get("product"),
        "owner_key": governance.get("owner_key"),
        "layer": governance.get("layer"),
        "classification": governance.get("classification"),
        "schema": node.get("schema"),
        "name": node.get("alias") or node.get("name"),
        "description": description,
        "documentation_status": status_da_tabela,
        # materialização do dbt: é o que diz se a entidade no catálogo é
        # tabela, view ou view materializada.
        "materialized": node.get("config", {}).get("materialized"),
        "depends_on": [
            dependency
            for dependency in node.get("depends_on", {}).get("nodes", [])
            if dependency.startswith("model.")
        ],
        "columns": columns,
        # separa as duas decisões que antes eram uma só: aparecer no catálogo
        # e poder alimentar RAG. A Bronze aparece; não alimenta.
        "rag_eligivel": elegivel_rag,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", type=Path, default=DBT_TARGET)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "dbt" / "mcid" / "governance" / "openmetadata_semantic_catalog.json",
    )
    args = parser.parse_args()

    manifest = load_json(args.target_dir / "manifest.json")
    catalog = load_json(args.target_dir / "catalog.json")
    schemas = load_yaml(GOVERNANCE / "schemas.yml").get("schemas", [])
    terms = load_yaml(GOVERNANCE / "glossary.yml").get("terms", [])

    violations: list[str] = []
    models = []
    for node_id, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") in EXCLUDED_RESOURCE_TYPES:
            continue
        if node.get("resource_type") != "model":
            continue
        exported = model_export(node_id, node, catalog, violations)
        if exported:
            models.append(exported)

    if violations:
        formatted = "\n- ".join(violations)
        raise RuntimeError(
            "Catálogo OpenMetadata não foi publicado; pendências de segurança ou "
            f"documentação encontradas:\n- {formatted}"
        )

    payload = {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "schemas": schemas,
        "glossary_terms": terms,
        "models": models,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Catálogo semântico seguro: {args.output} ({len(models)} models)")


if __name__ == "__main__":
    main()
