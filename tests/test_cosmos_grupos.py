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

Também confere as listas do `depende_de()` (macros/depende_de.sql). Teste singular sem `ref()`
não tem pai e o Cosmos nunca o agenda; a lista dá os pais, e um model que fica de fora dela
pode ser conferido antes de ser reconstruído. Cada lista tem de bater com o recorte calculado
do grafo.
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
# Toda DAG com task group do Cosmos (hoje a mcid_cosmos_dag e a conjuntura_dag).
DAGS_COM_COSMOS = sorted(
    p for p in (REPO_ROOT / "dags").rglob("*.py") if "DbtTaskGroup(" in p.read_text()
)
PROJETO_DBT = REPO_ROOT / "dbt" / "mcid"
TIPOS = ("model", "seed", "snapshot", "test", "source")
CHAVES = ("unique_id", "name", "resource_type", "fqn", "depends_on", "source_name", "meta")
PAIS = ("model.", "seed.", "snapshot.")


def _render_configs(dag: Path = DAG_COSMOS) -> tuple[list[dict[str, ast.expr]], dict[str, str]]:
    """Os argumentos do RenderConfig de cada DbtTaskGroup, e as constantes de texto do módulo."""
    arvore = ast.parse(dag.read_text())
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
    assert configs, f"nenhum DbtTaskGroup em {dag}"
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
            *CHAVES,
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
        # fonte não vira task: o Cosmos só a lê para montar o grafo
        if not uid.startswith("source.") and sum(uid in membros for membros in grupos.values()) != 1
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


@pytest.mark.parametrize("dag", DAGS_COM_COSMOS, ids=lambda p: p.stem)
def test_testes_de_varios_pais_rodam_em_task_propria(dag: Path) -> None:
    """Dentro de um grupo, o mesmo mecanismo puxa o teste para a task de cada pai."""
    configs, _ = _render_configs(dag)
    for config in configs:
        valor = config.get("should_detach_multiple_parents_tests")
        assert isinstance(valor, ast.Constant) and valor.value is True, (
            "RenderConfig precisa de should_detach_multiple_parents_tests=True: sem isso o "
            "teste com mais de um pai roda na task de cada pai, às vezes antes de o outro "
            "existir, e a falha dele bloqueia os filhos do pai"
        )


def test_as_duas_dags_do_cosmos_sao_conferidas() -> None:
    assert {p.stem for p in DAGS_COM_COSMOS} >= {"mcid_cosmos_dag", "conjuntura_dag"}


# --- listas do depende_de() -------------------------------------------------------------


def _modelos(nos: dict[str, dict], dominio: str) -> dict[str, dict]:
    """Models de uma pasta de models/ (`fqn` = [projeto, dominio, ...])."""
    return {
        uid: no
        for uid, no in nos.items()
        if no["resource_type"] == "model" and no["fqn"][1] == dominio
    }


def _ouros(nos: dict[str, dict], dominio: str) -> set[str]:
    return {uid for uid, no in _modelos(nos, dominio).items() if no["name"].startswith("ouro_")}


def _folhas(nos: dict[str, dict], dominio: str) -> set[str]:
    """Models do domínio sem filho no domínio: todo model dele é ancestral de uma folha."""
    modelos = _modelos(nos, dominio)
    com_filho = {pai for no in modelos.values() for pai in no["depends_on"]["nodes"]}
    return set(modelos) - com_filho


def _bronzes_com_piso(nos: dict[str, dict]) -> set[str]:
    """`bronze_<fonte>` de cada fonte de `lake_staging` com `meta.linhas_minimas`."""
    por_nome = {no["name"]: uid for uid, no in nos.items() if no["resource_type"] == "model"}
    return {
        por_nome.get(f"bronze_{no['name']}", f"(model inexistente) bronze_{no['name']}")
        for no in nos.values()
        if no["resource_type"] == "source"
        and no["source_name"] == "lake_staging"
        and (no.get("meta") or {}).get("linhas_minimas")
    }


# teste singular -> os models que a lista do depende_de() dele precisa ter, nem mais nem menos
RECORTES = {
    "ouro_sem_identificador_pessoal": lambda nos: _ouros(nos, "reforma_casa_brasil_dbt"),
    "linha_financiada_ouro_sem_identificador_pessoal": lambda nos: _ouros(
        nos, "linha_financiada_dbt"
    ),
    "conjuntura_contrato_do_staging": _bronzes_com_piso,
    # os três que usam a folhas_conjuntura()
    "conjuntura_padronizacao_de_colunas": lambda nos: _folhas(nos, "conjuntura_dbt"),
    "conjuntura_sem_dado_pessoal_no_conteudo": lambda nos: _folhas(nos, "conjuntura_dbt"),
    "conjuntura_sem_dado_sensivel": lambda nos: _folhas(nos, "conjuntura_dbt"),
}


@pytest.mark.parametrize("teste", sorted(RECORTES))
def test_depende_de_cobre_o_recorte_do_teste(grafo: tuple, teste: str) -> None:
    nos, _ = grafo
    [no] = [no for no in nos.values() if no["resource_type"] == "test" and no["name"] == teste]
    pais = {p for p in no["depends_on"]["nodes"] if p.startswith("model.")}
    esperado = RECORTES[teste](nos)
    faltam, sobram = sorted(esperado - pais), sorted(pais - esperado)
    assert not faltam and not sobram, (
        f"atualize o depende_de() de {teste} (ou a folhas_conjuntura(), em "
        f"macros/depende_de.sql):\n  faltam: {faltam}\n  sobram: {sobram}"
    )


def test_folhas_ignoram_filho_de_outro_dominio() -> None:
    def model(nome: str, dominio: str, *pais: str) -> tuple[str, dict]:
        no = {"resource_type": "model", "name": nome, "fqn": ["mcid", dominio, nome]}
        return f"model.mcid.{nome}", {**no, "depends_on": {"nodes": list(pais)}}

    nos = dict(
        [
            model("bronze_x", "conjuntura_dbt"),
            model("prata_x", "conjuntura_dbt", "model.mcid.bronze_x"),
            model("ouro_y", "outro_dbt", "model.mcid.prata_x"),
        ]
    )
    assert _folhas(nos, "conjuntura_dbt") == {"model.mcid.prata_x"}
