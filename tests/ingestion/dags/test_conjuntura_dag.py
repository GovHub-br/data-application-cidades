"""Orquestração do conjuntura: o que a conjuntura_dag dispara existe e roda no cron.

Lida por AST, sem importar a DAG (o import monta o projeto do Cosmos).
"""

import ast
from pathlib import Path

DAGS = Path(__file__).resolve().parents[3] / "dags"
CONJUNTURA = DAGS / "conjuntura" / "conjuntura_dag.py"


def _module(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _dag_calls(tree: ast.Module) -> list[tuple[str, ast.Call]]:
    """(dag_id, chamada do @dag) de cada função decorada com @dag(...)."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "dag"
            ):
                keywords = {k.arg: k.value for k in decorator.keywords}
                dag_id = keywords.get("dag_id")
                name = dag_id.value if isinstance(dag_id, ast.Constant) else node.name
                found.append((str(name), decorator))
    return found


def _triggered() -> list[str]:
    for node in _module(CONJUNTURA).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "INGEST_DAG_IDS" for t in node.targets
        ):
            return [ast.literal_eval(e) for e in node.value.elts]
    raise AssertionError("INGEST_DAG_IDS não encontrado")


def _defined_dag_ids() -> set[str]:
    """dag_id literal no @dag, ou string do módulo que monta DAGs num laço.

    Um arquivo pode montar várias DAGs a partir de uma tabela (o Novo CAGED): aí o
    dag_id é variável no @dag e aparece como string no próprio módulo.
    """
    defined: set[str] = set()
    for path in DAGS.rglob("*.py"):
        tree = _module(path)
        calls = _dag_calls(tree)
        defined.update(dag_id for dag_id, _ in calls)
        if calls:
            defined.update(
                node.value
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
            )
    return defined


def test_every_triggered_dag_exists_in_the_repository() -> None:
    defined = _defined_dag_ids()

    missing = [dag_id for dag_id in _triggered() if dag_id not in defined]

    assert not missing, f"a conjuntura_dag dispara DAGs que não existem: {missing}"


def test_weekly_schedule_is_a_literal_cron() -> None:
    [(_, call)] = _dag_calls(_module(CONJUNTURA))
    schedule = {k.arg: k.value for k in call.keywords}["schedule"]

    assert isinstance(schedule, ast.Constant) and schedule.value == "0 8 * * 1"
