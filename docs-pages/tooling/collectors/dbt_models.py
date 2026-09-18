"""
dbt_models.py: inventario e linhagem dos projetos dbt.

Le a arvore de arquivos, nao o manifest.json: a convencao de pastas do repo
(<projeto>/models/<dominio>_dbt/<camada>/<modelo>.sql) ja carrega projeto,
dominio e camada, e os ref()/source() no SQL dao a linhagem. Assim a coleta
roda offline, sem dbt instalado e sem conexao com o Postgres.

Saida: docs-pages/src/_data/dbt.json
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from tooling.common import DBT_DIR, ROOT_DIR, log

RE_REF = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
RE_SOURCE = re.compile(r"""source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")
CAMADAS = ("bronze", "silver", "gold")


def _descricoes(pasta: Path) -> dict[str, str]:
    """Extrai descricoes de modelos do schema.yml da pasta, se houver."""
    schema = pasta / "schema.yml"
    if not schema.exists():
        return {}
    try:
        dados = yaml.safe_load(schema.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as erro:
        log.warning("schema.yml invalido em %s: %s", pasta, erro)
        return {}
    return {
        modelo["name"]: (modelo.get("description") or "").strip()
        for modelo in dados.get("models") or []
        if isinstance(modelo, dict) and modelo.get("name")
    }


def _testes(pasta: Path) -> dict[str, int]:
    """Conta testes declarados por modelo no schema.yml da pasta."""
    schema = pasta / "schema.yml"
    if not schema.exists():
        return {}
    try:
        dados = yaml.safe_load(schema.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    contagem: dict[str, int] = {}
    for modelo in dados.get("models") or []:
        if not isinstance(modelo, dict) or not modelo.get("name"):
            continue
        total = len(modelo.get("tests") or modelo.get("data_tests") or [])
        for coluna in modelo.get("columns") or []:
            total += len(coluna.get("tests") or coluna.get("data_tests") or [])
        contagem[modelo["name"]] = total
    return contagem


def _classificar(relativo: Path) -> tuple[str, str]:
    """Devolve (dominio, camada) a partir do caminho relativo a models/."""
    partes = relativo.parts[:-1]
    if not partes:
        return "geral", "outros"
    dominio = partes[0].removesuffix("_dbt")
    camada = next((p for p in partes if p in CAMADAS), "outros")
    return dominio, camada


def coletar() -> dict[str, Any]:
    modelos: list[dict[str, Any]] = []
    arestas: list[dict[str, str]] = []
    fontes: set[str] = set()

    for projeto_dir in sorted(p for p in DBT_DIR.iterdir() if p.is_dir()):
        models_dir = projeto_dir / "models"
        if not models_dir.is_dir():
            continue
        projeto = projeto_dir.name

        for sql in sorted(models_dir.rglob("*.sql")):
            relativo = sql.relative_to(models_dir)
            dominio, camada = _classificar(relativo)
            nome = sql.stem
            conteudo = sql.read_text(encoding="utf-8", errors="replace")

            descricoes = _descricoes(sql.parent)
            testes = _testes(sql.parent)

            refs = sorted(set(RE_REF.findall(conteudo)))
            refs_fontes = sorted({f"{s}.{t}" for s, t in RE_SOURCE.findall(conteudo)})
            fontes.update(refs_fontes)

            modelos.append(
                {
                    "projeto": projeto,
                    "dominio": dominio,
                    "camada": camada,
                    "nome": nome,
                    "caminho": str(sql.relative_to(ROOT_DIR)),
                    "descricao": descricoes.get(nome, ""),
                    "testes": testes.get(nome, 0),
                    "linhas": conteudo.count("\n") + 1,
                    "refs": refs,
                    "sources": refs_fontes,
                }
            )
            for origem in refs:
                arestas.append(
                    {"de": origem, "para": nome, "projeto": projeto, "dominio": dominio}
                )

    por_camada: dict[str, int] = {}
    por_projeto: dict[str, int] = {}
    por_dominio: dict[str, int] = {}
    for modelo in modelos:
        por_camada[modelo["camada"]] = por_camada.get(modelo["camada"], 0) + 1
        por_projeto[modelo["projeto"]] = por_projeto.get(modelo["projeto"], 0) + 1
        por_dominio[modelo["dominio"]] = por_dominio.get(modelo["dominio"], 0) + 1

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "resumo": {
            "total_modelos": len(modelos),
            "total_testes": sum(m["testes"] for m in modelos),
            "total_dominios": len(por_dominio),
            "total_fontes": len(fontes),
            "por_camada": por_camada,
            "por_projeto": por_projeto,
            "por_dominio": dict(sorted(por_dominio.items(), key=lambda x: -x[1])),
        },
        "modelos": modelos,
        "linhagem": arestas,
        "fontes": sorted(fontes),
    }
