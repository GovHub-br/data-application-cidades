#!/usr/bin/env python3
"""Lê as restrições que o dbt já declara e as traduz para o catálogo.

O dbt não tem `primary key`: tem teste. Uma coluna com `unique` **e**
`not_null` é, na prática, a chave — e é isso que o catálogo precisa mostrar
para quem quer entender a granularidade sem abrir o SQL.

    unique + not_null  ->  PRIMARY_KEY, em `tableConstraints`
    unique             ->  UNIQUE, na coluna
    not_null           ->  NOT_NULL, na coluna

Lê os `schema.yml` dos models, que são declaração versionada. Não toca no
manifest nem em dado.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import yaml

from governanca_comum import RAIZ

MODELOS = RAIZ / "dbt" / "mcid" / "models"


@dataclass
class Restricoes:
    """O que o dbt declara sobre as colunas de um model."""

    unicas: set[str] = field(default_factory=set)
    nao_nulas: set[str] = field(default_factory=set)

    @property
    def chave_primaria(self) -> list[str]:
        """Colunas que são únicas E não nulas — a chave, na prática."""
        return sorted(self.unicas & self.nao_nulas)

    def restricao_da_coluna(self, coluna: str) -> str | None:
        """Restrição no nível da coluna.

        NUNCA devolve `PRIMARY_KEY`: a instância recusa a tabela se a mesma
        coluna vier marcada como chave aqui E em `tableConstraints` ("A column
        already tagged as a primary key and table constraint also includes
        primary key"). A chave fica só em `tableConstraints`, que é onde cabe
        chave composta; a coluna da chave recebe `NOT_NULL`, que continua
        verdadeiro e não conflita.
        """
        if coluna in self.nao_nulas:
            return "NOT_NULL"
        if coluna in self.unicas:
            return "UNIQUE"
        return None


def _nomes_de_teste(entrada: object) -> list[str]:
    """`tests:` aceita string simples ou dicionário com configuração."""
    if isinstance(entrada, str):
        return [entrada]
    if isinstance(entrada, dict):
        return list(entrada)
    return []


def carregar() -> dict[str, Restricoes]:
    """Restrições declaradas, por nome de model."""
    resultado: dict[str, Restricoes] = {}
    for caminho in sorted(MODELOS.rglob("*.yml")):
        try:
            documento = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        for modelo in documento.get("models") or []:
            restricoes = resultado.setdefault(modelo["name"], Restricoes())
            for coluna in modelo.get("columns") or []:
                testes = coluna.get("tests") or coluna.get("data_tests") or []
                for teste in testes:
                    for nome in _nomes_de_teste(teste):
                        if nome == "unique":
                            restricoes.unicas.add(coluna["name"])
                        elif nome == "not_null":
                            restricoes.nao_nulas.add(coluna["name"])
    return {nome: r for nome, r in resultado.items() if r.unicas or r.nao_nulas}


def constraints_da_tabela(
    restricoes: Restricoes, colunas_publicadas: set[str]
) -> list[dict]:
    """`tableConstraints` no formato do OpenMetadata.

    Só declara chave sobre coluna que EXISTE no catálogo. Uma coluna omitida
    por ser identificador sensível não pode virar chave publicada: o catálogo
    apontaria para um campo que ele mesmo não mostra.
    """
    chave = [c for c in restricoes.chave_primaria if c in colunas_publicadas]
    if not chave:
        return []
    return [{"constraintType": "PRIMARY_KEY", "columns": chave}]


if __name__ == "__main__":
    todas = carregar()
    print(f"models com restrição declarada: {len(todas)}")
    for nome, r in sorted(todas.items()):
        pk = r.chave_primaria
        print(
            f"  {nome:34} PK={pk or '-'}  únicas={sorted(r.unicas) or '-'}  "
            f"não-nulas={len(r.nao_nulas)}"
        )
