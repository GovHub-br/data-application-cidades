"""DAG do IBGE (API v3) no pipeline novo: a lista de agregados vive no código."""

from typing import Any

import pytest

from ingestion.loaders import LoadMode
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)

TABELAS = {
    "pib_construcao",
    "sinapi",
    "pib_consolidado_trimestral_bruto",
    "pib_corrente_milhoes_brl",
    "paic_resultados",
    "paic_pessoal_salarial",
    "paic_obras",
    "pnad_trabalho_construcao",
    "pnad_rendimento_construcao",
    "pnadc_populacao_decis_renda",
    "pnadc_rendimento_domiciliar_real",
    "pnadc_habitacao_condicao",
    "ibge_pim_pf_brasil",
    "ibge_pmc_construcao",
}


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/ibge/ibge_ingest_dag.py")


def test_dag_identity_and_one_task_group_per_dataset(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "ibge_ingest_dag"
    assert dag.schedule == "0 6 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {"ibge", "conjuntura", "ingestion"} <= set(dag.tags)
    assert {spec.dataset for spec in module.DATASETS} == TABELAS
    for dataset in TABELAS:
        convert = dag.task_dict[f"{dataset}.convert_to_staging"]
        assert convert.upstream_task_ids == {f"{dataset}.extract_to_raw"}


def test_dataset_with_classification(module: Any) -> None:
    spec = {s.dataset: s for s in module.DATASETS}["pib_construcao"]
    config = spec.extractor_config()
    [request] = config.requests

    assert config.base_url == "https://servicodados.ibge.gov.br"
    assert request.endpoint == (
        "/api/v3/agregados/5932/periodos/-20/variaveis/6564|6563|6562|6561"
    )
    assert dict(request.params) == {
        "localidades": "N1[1]",
        "classificacao": "11255[90694]",
    }
    assert spec.converter.nested == ("resultados", "series")
    assert spec.converter.explode_keys == "serie"
    # Janela parcial (-20 períodos): o histórico se acumula pelo merge.
    assert spec.load_mode is LoadMode.MERGE
    assert spec.keys == (
        "variavel_id",
        "localidade_id",
        "classificacao_id",
        "categoria_id",
        "periodo",
    )


def test_dataset_without_classification_and_with_two_categories(module: Any) -> None:
    specs = {s.dataset: s for s in module.DATASETS}
    [sinapi] = specs["sinapi"].extractor_config().requests
    [pnad] = specs["pnad_trabalho_construcao"].extractor_config().requests

    assert dict(sinapi.params) == {"localidades": "N1[1]"}
    assert sinapi.endpoint.endswith("/2296/periodos/-30/variaveis/48|1196|1197|1198")
    # A API separa variáveis por "|" e categorias por ",".
    assert dict(pnad.params)["classificacao"] == "888[47946,47949]"


def test_tasks_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    for spec in module.DATASETS:
        raw = run_task(dag, f"{spec.dataset}.extract_to_raw")
        run_task(dag, f"{spec.dataset}.convert_to_staging", raw)

    assert calls == [
        call
        for spec in module.DATASETS
        for call in (
            ("extract_to_raw", spec, RUN_AFTER),
            ("convert_to_staging", spec, "raw/x/"),
        )
    ]
