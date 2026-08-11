"""
airflow_dags.py: inventario das DAGs e dos clientes de ingestao.

Le o codigo com ast, sem importar Airflow: o parse e estatico e roda offline.

Saida: docs-pages/src/_data/airflow.json
"""

import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tooling.common import DAGS_DIR, PLUGINS_DIR, ROOT_DIR, log

NOMES_DAG = {"DAG", "dag", "DbtDag"}


def _literal(no: ast.AST) -> Any:
    try:
        return ast.literal_eval(no)
    except (ValueError, SyntaxError):
        return None


def _nome_chamado(no: ast.AST) -> str:
    """Nome do callable em Name, Attribute ou Call (ex.: dag, DAG, DbtDag)."""
    if isinstance(no, ast.Call):
        return _nome_chamado(no.func)
    if isinstance(no, ast.Attribute):
        return no.attr
    if isinstance(no, ast.Name):
        return no.id
    return ""


def _dag_de_arquivo(caminho: Path) -> dict[str, Any] | None:
    """Extrai dag_id, schedule, tags e docstring de um arquivo de DAG."""
    try:
        arvore = ast.parse(caminho.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError as erro:
        log.warning("nao foi possivel parsear %s: %s", caminho, erro)
        return None

    info: dict[str, Any] = {
        "arquivo": str(caminho.relative_to(ROOT_DIR)),
        "grupo": caminho.parent.relative_to(DAGS_DIR).as_posix(),
        "dag_id": caminho.stem,
        "schedule": None,
        "tags": [],
        "descricao": (ast.get_docstring(arvore) or "").strip().split("\n")[0],
    }

    encontrou = False
    for no in ast.walk(arvore):
        argumentos: list[ast.keyword] = []

        if isinstance(no, ast.Call) and _nome_chamado(no) in NOMES_DAG:
            encontrou = True
            argumentos = no.keywords
        elif isinstance(no, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorador = next(
                (d for d in no.decorator_list if _nome_chamado(d) in NOMES_DAG), None
            )
            if decorador is None:
                continue
            encontrou = True
            info["dag_id"] = no.name
            info["descricao"] = (ast.get_docstring(no) or info["descricao"]).split("\n")[
                0
            ]
            if isinstance(decorador, ast.Call):
                argumentos = decorador.keywords
        else:
            continue

        _aplicar_argumentos(info, argumentos)
        break

    return info if encontrou else None


def _aplicar_argumentos(info: dict[str, Any], argumentos: list[ast.keyword]) -> None:
    """Copia dag_id, schedule, tags e description dos kwargs para info."""
    for kw in argumentos:
        if kw.arg == "dag_id":
            info["dag_id"] = _literal(kw.value) or info["dag_id"]
        elif kw.arg in {"schedule", "schedule_interval"}:
            # get_dynamic_schedule() resolve o cron em runtime, pela Variable
            # dynamic_schedules: o codigo nao carrega o valor.
            if _nome_chamado(kw.value) == "get_dynamic_schedule":
                info["schedule"] = "dinamico"
            else:
                info["schedule"] = _literal(kw.value)
        elif kw.arg == "tags":
            info["tags"] = _literal(kw.value) or []
        elif kw.arg == "description" and not info["descricao"]:
            info["descricao"] = _literal(kw.value) or ""


def _clientes() -> list[dict[str, Any]]:
    """Lista os clientes de ingestao em plugins/, com suas classes e metodos."""
    clientes: list[dict[str, Any]] = []
    for arquivo in sorted(PLUGINS_DIR.glob("cliente_*.py")):
        try:
            arvore = ast.parse(arquivo.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        classes = [n for n in ast.walk(arvore) if isinstance(n, ast.ClassDef)]
        metodos = [
            n.name
            for classe in classes
            for n in classe.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not n.name.startswith("_")
        ]
        clientes.append(
            {
                "sistema": arquivo.stem.removeprefix("cliente_"),
                "arquivo": str(arquivo.relative_to(ROOT_DIR)),
                "classes": [c.name for c in classes],
                "metodos": metodos,
                "total_metodos": len(metodos),
                "descricao": (ast.get_docstring(arvore) or "").strip().split("\n")[0],
            }
        )
    return clientes


def coletar() -> dict[str, Any]:
    dags: list[dict[str, Any]] = []
    for arquivo in sorted(DAGS_DIR.rglob("*_dag.py")):
        dag = _dag_de_arquivo(arquivo)
        if dag:
            dags.append(dag)

    clientes = _clientes()
    por_grupo: dict[str, int] = {}
    for dag in dags:
        raiz = dag["grupo"].split("/")[0]
        por_grupo[raiz] = por_grupo.get(raiz, 0) + 1

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "resumo": {
            "total_dags": len(dags),
            "total_clientes": len(clientes),
            "por_grupo": dict(sorted(por_grupo.items(), key=lambda x: -x[1])),
        },
        "dags": dags,
        "clientes": clientes,
    }
