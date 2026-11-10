"""Toda DAG importa sem erro e sem banco (ADR-0013).

Pega import que não existe mais no Airflow da imagem, dependência que falta no runtime e
leitura de configuração que quebra o parse.
"""

from pathlib import Path

from tests.conftest import TEST_ENV

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_todas_as_dags_importam(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Vem do deploy (compose, playbook, k8s): a raiz do repositório sincronizado.
    monkeypatch.setenv("AIRFLOW_REPO_BASE", str(REPO_ROOT))
    # O Cosmos roda `dbt ls` no parse (o profiles.yml exige as credenciais, mas não conecta)
    # e guardaria o resultado numa Variable do banco, que o teste não tem.
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    # Os dois `dbt ls` passam dos 30 s padrão por arquivo numa máquina lenta da CI.
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "300")
    for chave, valor in TEST_ENV.items():
        monkeypatch.setenv(chave, valor)
    from airflow.dag_processing.dagbag import DagBag

    dagbag = DagBag(dag_folder=str(REPO_ROOT / "dags"))

    assert not dagbag.import_errors, "\n\n".join(
        f"{arquivo}:\n{erro}" for arquivo, erro in dagbag.import_errors.items()
    )
    assert dagbag.dags
