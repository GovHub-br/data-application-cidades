#!/usr/bin/env python3
"""Deriva a linhagem coluna a coluna dos models do dbt.

A linhagem de tabela responde "de onde vem esta tabela". Esta responde "de
onde vem ESTE número", que é a pergunta de quem vai mexer num model e precisa
saber o que quebra.

**Nada de SQL é publicado.** O SQL é lido e analisado aqui, localmente, e o que
sai é apenas o mapeamento `coluna de origem -> coluna de destino`. É a mesma
regra do catálogo semântico: publica-se significado e relação, nunca a consulta.

Por que dá para ler o SQL do repo: são os arquivos do model, que já são
declaração versionada — não o `compiled_code` do manifest, que carrega
referências de camadas restritas.

O `select *` de uma CTE só se resolve com o schema em mãos. Ele vem do catálogo
semântico já filtrado, que é a lista autorizada de colunas — nenhuma coluna
restrita entra na análise porque nenhuma está lá.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage

from governanca_comum import RAIZ

MODELOS = RAIZ / "dbt" / "mcid" / "models"
CATALOGO = RAIZ / "dbt" / "mcid" / "governance" / "openmetadata_semantic_catalog.json"

REF = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
#: O que sobra de Jinja depois de resolver o `ref` é configuração e macro, que
#: não participam da projeção de colunas.
JINJA = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.S)


@dataclass
class Vinculo:
    """Uma coluna de destino e as colunas de origem que a produzem."""

    modelo_destino: str
    coluna_destino: str
    origens: set[tuple[str, str]]


def sql_analisavel(bruto: str) -> str:
    """Troca `ref()` pelo nome do model e remove o Jinja restante."""
    return JINJA.sub("", REF.sub(lambda m: m.group(1), bruto))


def schema_do_catalogo() -> dict[str, dict[str, str]]:
    """Colunas por model, vindas do catálogo semântico já filtrado."""
    if not CATALOGO.exists():
        return {}
    catalogo = json.loads(CATALOGO.read_text(encoding="utf-8"))
    return {
        modelo["name"]: {coluna["name"]: "TEXT" for coluna in modelo["columns"]}
        for modelo in catalogo.get("models", [])
    }


def derivar(schema: dict[str, dict[str, str]]) -> tuple[list[Vinculo], list[str]]:
    """Percorre os models e devolve os vínculos e os que não deram para ler."""
    vinculos: list[Vinculo] = []
    ilegiveis: list[str] = []
    for arquivo in sorted(MODELOS.rglob("*.sql")):
        modelo = arquivo.stem
        if modelo not in schema:
            continue  # não é publicado no catálogo; não há o que ligar
        sql = sql_analisavel(arquivo.read_text(encoding="utf-8"))
        try:
            arvore = parse_one(sql, dialect="postgres")
        except SqlglotError:
            ilegiveis.append(modelo)
            continue
        if arvore is None or not isinstance(arvore, exp.Query):
            ilegiveis.append(modelo)
            continue
        for projecao in arvore.selects:
            coluna = projecao.alias_or_name
            if not coluna or coluna == "*":
                continue
            origens = _origens(coluna, sql, schema, modelo)
            if origens:
                vinculos.append(Vinculo(modelo, coluna, origens))
    return vinculos, ilegiveis


def _origens(
    coluna: str, sql: str, schema: dict[str, dict[str, str]], modelo: str
) -> set[tuple[str, str]]:
    """Colunas de outros models que produzem esta coluna."""
    try:
        no = lineage(coluna, sql, schema=schema, dialect="postgres")
    except (SqlglotError, KeyError, ValueError):
        return set()
    origens: set[tuple[str, str]] = set()
    for atual in no.walk():
        if not (atual.source and isinstance(atual.source, exp.Table)):
            continue
        if "." not in atual.name:
            continue
        tabela, campo = atual.name.rsplit(".", 1)
        # só liga a outro model publicado, e nunca o model a si mesmo
        if tabela in schema and tabela != modelo and campo in schema[tabela]:
            origens.add((tabela, campo))
    return origens


def agrupar_por_aresta(
    vinculos: list[Vinculo],
) -> dict[tuple[str, str], list[tuple[list[str], str]]]:
    """Junta os vínculos por par (model de origem, model de destino).

    O OpenMetadata guarda a linhagem de coluna dentro da aresta entre as duas
    tabelas — uma aresta carrega todas as colunas que a atravessam.
    """
    arestas: dict[tuple[str, str], dict[str, list[str]]] = {}
    for vinculo in vinculos:
        for modelo_origem, coluna_origem in vinculo.origens:
            chave = (modelo_origem, vinculo.modelo_destino)
            colunas = arestas.setdefault(chave, {}).setdefault(vinculo.coluna_destino, [])
            if coluna_origem not in colunas:
                colunas.append(coluna_origem)
    return {
        chave: [(sorted(origens), destino) for destino, origens in colunas.items()]
        for chave, colunas in arestas.items()
    }


if __name__ == "__main__":
    esquema = schema_do_catalogo()
    achados, falharam = derivar(esquema)
    arestas = agrupar_por_aresta(achados)
    print(f"models no catálogo: {len(esquema)}")
    print(f"colunas com origem derivada: {len(achados)}")
    print(f"arestas com linhagem de coluna: {len(arestas)}")
    print(f"models que o parser não leu: {len(falharam)}")
    if falharam:
        print("  " + ", ".join(falharam[:10]))
