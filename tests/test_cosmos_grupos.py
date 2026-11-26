"""Nenhum teste do dbt roda num grupo do Cosmos diferente do grupo de um dos seus pais.

O Cosmos roda `dbt test --select <model>` na task de cada model, e a seleção indireta (eager)
do dbt puxa todo teste que toca aquele model, mesmo os que o seletor do grupo deixa de fora.
Um teste que liga nós de dois grupos roda então na task do primeiro grupo, antes de o segundo
construir a tabela. Foi o que travou o mcid_cosmos_dag até 11/2026: testes do eixo histórico
rodavam no grupo do Postgres, falhavam com `relation ... does not exist`, e a falha deles
impedia o grupo que criaria as tabelas.

Os grupos são os seletores que a DAG passa ao RenderConfig, lidos do código-fonte (importar a
DAG renderiza o Cosmos, que já é o trabalho do test_dagbag). Hoje é um grupo só; os testes
valem para o dia em que o projeto for repartido de novo.
"""

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import TEST_ENV

REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_COSMOS = REPO_ROOT / "dags" / "snh" / "dbt" / "mcid_cosmos_dag.py"
PROJETO_DBT = REPO_ROOT / "dbt" / "mcid"
TIPOS = ("model", "seed", "snapshot", "test")
PAIS = ("model.", "seed.", "snapshot.")


def _render_configs() -> tuple[list[dict[str, ast.expr]], dict[str, str]]:
    """Os argumentos do RenderConfig de cada DbtTaskGroup, e as constantes de texto do módulo."""
    arvore = ast.parse(DAG_COSMOS.read_text())
    constantes = {
        alvo.id: no.value.value
        for no in arvore.body
        if isinstance(no, ast.Assign) and isinstance(no.value, ast.Constant)
        for alvo in no.targets
        if isinstance(alvo, ast.Name) and isinstance(no.value.value, str)
    }
    configs = []
    for no in ast.walk(arvore):
        if isinstance(no, ast.Call) and getattr(no.func, "id", None) == "DbtTaskGroup":
            render = {k.arg: k.value for k in no.keywords}.get("render_config")
            assert (
                isinstance(render, ast.Call) and getattr(render.func, "id", None) == "RenderConfig"
            ), "todo DbtTaskGroup precisa de render_config=RenderConfig(...)"
            configs.append({k.arg: k.value for k in render.keywords if k.arg})
    assert configs, f"nenhum DbtTaskGroup em {DAG_COSMOS}"
    return configs, constantes


def _seletores() -> list[str]:
    configs, constantes = _render_configs()
    seletores = []
    for config in configs:
        assert not {"select", "exclude"} & set(config), (
            "o grupo deve vir de um seletor do dbt/mcid/selectors.yml (`selector=`), que é o "
            "que este teste sabe resolver"
        )
        valor = config.get("selector")
        if isinstance(valor, ast.Name):
            seletores.append(constantes[valor.id])
        elif isinstance(valor, ast.Constant) and isinstance(valor.value, str):
            seletores.append(valor.value)
        else:
            pytest.fail("RenderConfig sem `selector=` em texto ou constante do módulo")
    return seletores


def _dbt_ls(tmp: Path, *args: str) -> list[dict]:
    # `dbt ls` não conecta no banco; as credenciais falsas só satisfazem o profiles.yml.
    saida = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "ls",
            "--project-dir",
            str(PROJETO_DBT),
            "--profiles-dir",
            str(PROJETO_DBT),
            "--target-path",
            str(tmp / "target"),
            "--log-path",
            str(tmp / "logs"),
            "--quiet",
            "--resource-types",
            *TIPOS,
            "--output",
            "json",
            "--output-keys",
            "unique_id",
            "depends_on",
            *args,
        ],
        env={**os.environ, **TEST_ENV},
        capture_output=True,
        text=True,
        check=False,
    )
    assert saida.returncode == 0, saida.stdout + saida.stderr
    return [json.loads(linha) for linha in saida.stdout.splitlines() if linha.startswith("{")]


def _sem_grupo_unico(nos: dict[str, dict], grupos: dict[str, set[str]]) -> list[str]:
    """Nós fora de todo grupo (nunca rodam) ou em mais de um (rodam duas vezes)."""
    return [
        f"{uid}: {sorted(g for g, membros in grupos.items() if uid in membros) or 'nenhum'}"
        for uid in sorted(nos)
        if sum(uid in membros for membros in grupos.values()) != 1
    ]


def _cruzamentos(nos: dict[str, dict], grupos: dict[str, set[str]]) -> list[str]:
    """Testes com algum pai (model, seed, snapshot) num grupo diferente do seu."""
    grupo_de = {uid: g for g, membros in grupos.items() for uid in membros}
    problemas = []
    for uid, no in sorted(nos.items()):
        if not uid.startswith("test."):
            continue
        pais = [p for p in no["depends_on"]["nodes"] if p.startswith(PAIS)]
        fora = sorted(
            f"{p} ({grupo_de.get(p)})" for p in pais if grupo_de.get(p) != grupo_de.get(uid)
        )
        if fora:
            problemas.append(f"{uid} ({grupo_de.get(uid)}): {', '.join(fora)}")
    return problemas


@pytest.fixture(scope="module")
def grafo(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, dict], dict[str, set[str]]]:
    tmp = tmp_path_factory.mktemp("dbt_ls")
    nos = {no["unique_id"]: no for no in _dbt_ls(tmp)}
    grupos = {s: {no["unique_id"] for no in _dbt_ls(tmp, "--selector", s)} for s in _seletores()}
    return nos, grupos


def test_cada_no_do_dbt_fica_em_um_grupo_so(grafo: tuple) -> None:
    nos, grupos = grafo
    problemas = _sem_grupo_unico(nos, grupos)
    assert not problemas, "nós fora de um grupo único do Cosmos:\n" + "\n".join(problemas)


def test_nenhum_teste_cruza_grupos(grafo: tuple) -> None:
    nos, grupos = grafo
    problemas = _cruzamentos(nos, grupos)
    assert not problemas, (
        "testes num grupo do Cosmos diferente do de um dos pais (rodariam antes de o pai do "
        "outro grupo existir):\n" + "\n".join(problemas)
    )


def test_cruzamento_entre_grupos_e_detectado() -> None:
    """O caso que travou a DAG: teste de domínio ligando um seed do Postgres a um model do eixo."""
    nos = {
        "seed.mcid.dominio_status": {"depends_on": {"nodes": []}},
        "model.mcid.prata_historico": {"depends_on": {"nodes": []}},
        "test.mcid.dominio_status_test": {
            "depends_on": {"nodes": ["seed.mcid.dominio_status", "model.mcid.prata_historico"]}
        },
    }
    grupos = {
        "postgres": {"seed.mcid.dominio_status", "test.mcid.dominio_status_test"},
        "duckdb": {"model.mcid.prata_historico"},
    }
    assert _cruzamentos(nos, grupos) == [
        "test.mcid.dominio_status_test (postgres): model.mcid.prata_historico (duckdb)"
    ]
    assert _cruzamentos(nos, {"um_grupo": set(nos)}) == []


def test_testes_de_varios_pais_rodam_em_task_propria() -> None:
    """Dentro de um grupo, o mesmo mecanismo puxa o teste para a task de cada pai."""
    configs, _ = _render_configs()
    for config in configs:
        valor = config.get("should_detach_multiple_parents_tests")
        assert isinstance(valor, ast.Constant) and valor.value is True, (
            "RenderConfig precisa de should_detach_multiple_parents_tests=True: sem isso o "
            "teste com mais de um pai roda na task de cada pai, às vezes antes de o outro existir"
        )
