"""DAG do SFTP: as famílias da Variable SFTP_FAMILIAS contra a árvore real da fábrica.

As famílias são o valor canônico da Variable (`infra/airflow/variables/
SFTP_FAMILIAS.json`); a árvore é a listagem do SFTP (caminho, tamanho, data) em
2026-10-09. Os testes passam a seleção de cada família sobre ela: todo arquivo cai
em uma família só, ou é um avulso declarado aqui.
"""

import dataclasses
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.extractors import RemoteFiles
from ingestion.extractors.models import sftp_extractor
from ingestion.loaders import LoadMode
from ingestion.prepare import MaskPii, Unpack
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    nothing_new,
    record_steps,
    run_task,
)

ARVORE = Path(__file__).parent / "fixtures" / "sftp_fabrica_arvore.tsv"
REPO = Path(__file__).resolve().parents[3]
FAMILIAS = json.loads((REPO / "infra/airflow/variables/SFTP_FAMILIAS.json").read_text())
# A configuração de cada família antes da Variable (tupla na DAG).
ANTES = json.loads(
    (Path(__file__).parent / "fixtures/sftp_familias_antes.json").read_text()
)

# O que nenhuma família pega, e por quê.
AVULSOS = (
    r"(?i)substitu",  # versão substituída de uma entrega
    r"\.filepart$",  # upload em curso
    r"\.7z$",  # vazios
    r"/CORREÇÃO_FASE_PROJETO\.xlsx$",  # planilha solta
    r"/RELAÇÃO_APF_FASES_FDS_[^/]*\.xlsx$",  # planilha solta
    r"/Teste_Transmissao\.txt$",
    r"/Legislacao\.zip$",  # PDFs
    r"/\d{6}_Validacoes_PMCMV_MAR_2020\.xlsx$",  # análise avulsa de 2020
)


class FakeVariable:
    reads: list[str] = []

    @classmethod
    def get(cls, key: str, **kwargs: Any) -> Any:
        cls.reads.append(key)
        return FAMILIAS


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = load_dag_module("data_ingest/sftp/sftp_ingest_dag.py")
    monkeypatch.setattr(FakeVariable, "reads", [])
    monkeypatch.setattr(loaded, "Variable", FakeVariable)
    return loaded


@pytest.fixture
def specs(module: Any) -> dict[str, Any]:
    """Uma família resolvida por entrada da Variable."""
    return {f["dataset"]: module.FAMILIA.resolve(f) for f in FAMILIAS}


def _arvore() -> list[tuple[str, int, int]]:
    rows = []
    for line in ARVORE.read_text().splitlines():
        if line and not line.startswith("#"):
            path, size, mtime = line.split("\t")
            rows.append((path, int(size), int(mtime)))
    return rows


def _listing(root: str) -> list[Any]:
    """A árvore vista de `root`, como o `_walk` do extrator a entrega."""
    folder = root.removeprefix("/home/fabrica/") + "/"
    return [
        sftp_extractor._Remote(
            path=f"/home/fabrica/{path}",
            relative=path[len(folder) :],
            size=size,
            mtime=mtime,
        )
        for path, size, mtime in _arvore()
        if path.startswith(folder)
    ]


def _selection(spec: Any) -> tuple[list[Any], list[Any]]:
    """(entregas soltas, pacotes) que o extrator baixaria na primeira execução."""
    query: RemoteFiles = spec.extractor_config().remote
    listing = _listing(query.root)
    loose = sftp_extractor._select(listing, query)
    bundles = []
    if query.bundles:
        as_bundles = dataclasses.replace(
            query, pattern=query.bundles, prefer_extensions=()
        )
        bundles = sftp_extractor._select(listing, as_bundles)
    return loose, bundles


def _identity(name: str) -> str:
    return sftp_extractor._identity(
        name, {ext: i for i, ext in enumerate((".csv", ".txt", ".xlsx", ".zip", ".gz"))}
    )


@pytest.fixture
def selections(specs: dict[str, Any]) -> dict[str, tuple[list[Any], list[Any]]]:
    return {dataset: _selection(spec) for dataset, spec in specs.items()}


def test_two_declared_tasks(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "sftp_ingest_dag"
    assert dag.schedule == "0 1 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.default_args["owner"] == "Gustavo"
    assert {"sftp", "ingestion"} <= set(dag.tags)
    assert set(dag.task_dict) == {"extract_to_raw", "convert_to_staging"}
    assert dag.task_dict["convert_to_staging"].upstream_task_ids == {"extract_to_raw"}


def test_each_family_downloads_and_prepares_as_before(specs: dict[str, Any]) -> None:
    assert list(specs) == list(ANTES)
    for dataset, spec in specs.items():
        remote = spec.extractor_config().remote
        unpack, _ = spec.prepare
        assert {
            "root": remote.root,
            "pattern": remote.pattern,
            "bundles": remote.bundles,
            "exclude": list(remote.exclude),
            "prefer": list(remote.prefer_extensions),
            "members": unpack.members,
            "require_match": unpack.require_match,
        } == ANTES[dataset], dataset


def test_a_wrong_family_is_an_error(module: Any) -> None:
    with pytest.raises(ValueError, match="inválida"):
        module.arquivos({"dataset": "x", "nome": "X", "pasta": "GEAVO"})
    with pytest.raises(ValueError, match="inválida"):
        module.arquivos({"dataset": "x"})


def test_every_family_is_incremental_overwrite_masked_and_auto_dialect(
    specs: dict[str, Any],
) -> None:
    for spec in specs.values():
        config = spec.extractor_config()
        assert spec.domain == "sftp"
        assert config.source == "sftp" and config.conn_id == "sftp_mcid"
        # retrato completo: o latest/ fica com a entrega mais recente
        assert spec.load_mode is LoadMode.OVERWRITE
        unpack, mask = spec.prepare
        assert isinstance(unpack, Unpack) and isinstance(mask, MaskPii)
        assert spec.converter.encoding == "auto"
        assert spec.converter.delimiter == "auto"
        assert spec.converter.bad_rows == "skip"


def test_every_file_of_the_tree_falls_in_one_family_or_is_a_declared_avulso(
    selections: dict[str, tuple[list[Any], list[Any]]],
) -> None:
    owners: dict[str, set[str]] = defaultdict(set)
    bundled: set[str] = set()
    for dataset, (loose, bundles) in selections.items():
        for remote in loose:
            owners[_identity(remote.name)].add(dataset)
        bundled.update(remote.relative for remote in bundles)

    orphans, shared = [], []
    for path, _, _ in _arvore():
        avulso = any(re.search(rx, path) for rx in AVULSOS)
        datasets = owners.get(_identity(path.rsplit("/", 1)[-1]), set())
        if avulso:
            assert not datasets, f"{path} é avulso, mas {datasets} o pegam"
        elif path.endswith(tuple(bundled)):
            continue
        elif not datasets:
            orphans.append(path)
        elif len(datasets) > 1:
            shared.append((path, datasets))

    assert orphans == []
    assert shared == []


def test_snh_caixa_does_not_take_the_deliveries_file(
    selections: dict[str, tuple[list[Any], list[Any]]],
) -> None:
    caixa, _ = selections["snh_dados_prioritarios_af_caixa"]
    entregas, _ = selections["snh_dados_prioritarios_af_caixa_entregas"]

    assert caixa and entregas
    assert not any("ENTREGAS" in r.name for r in caixa)
    assert all(r.name.endswith("_AF_CAIXA_ENTREGAS.csv") for r in entregas)
    # `<aaaa>_<mm>_` e `<aaaamm>_` são a mesma família
    caixa_bb, _ = selections["snh_dados_prioritarios_af_bb"]
    assert {r.name[:5] for r in caixa_bb} >= {"2024_", "20250"}


def test_one_delivery_per_identity_with_the_old_precedence(
    selections: dict[str, tuple[list[Any], list[Any]]],
) -> None:
    bb, _ = selections["snh_dados_prioritarios_af_bb"]
    names = {r.name for r in bb}

    # 202507 veio em .txt e .xlsx: fica o texto
    assert "202507_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.txt" in names
    assert "202507_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.xlsx" not in names
    # a base PF do FGTS está em GEFUS/ e GEFUS/FDS/ com o mesmo nome: uma vez só
    pf, _ = selections["geavo_base_pf_fgts"]
    assert len({r.name for r in pf}) == len(pf)


def test_int_families_take_the_2023_bundles_and_only_their_members(
    specs: dict[str, Any], selections: dict[str, tuple[list[Any], list[Any]]]
) -> None:
    pacotes = {"202310.zip", "202311.zip", "202312_CAIXA.zip"}

    for dataset, (_, bundles) in selections.items():
        if dataset.startswith("int") and dataset != "int055_liberacoes_caixa_bb":
            if dataset != "int068_solicitacao_liberacao_obra":
                assert {r.name for r in bundles} == pacotes, dataset
        else:
            assert bundles == [], dataset

    unpack = specs["int040_far_caixa_empreendimentos"].prepare[0]
    assert unpack.require_match is False
    members = re.compile(unpack.members)
    assert members.search(
        "INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS_20231228.TXT"
    )
    assert not members.search("INT039_MinisterioCidades_FAR_CAIXA_PF_20231228.TXT")


def test_variants_stay_in_the_family_and_substituted_ones_do_not(
    selections: dict[str, tuple[list[Any], list[Any]]],
) -> None:
    int057, _ = selections["int057_pnhr_bb_empreendimentos"]
    names = " ".join(r.name for r in int057)

    assert "_VALIDACAO_" in names
    assert "_ALTERACAO_LAYOUT_PARA_VALIDACAO_" in names
    assert "SUBSTITUIDO" not in names.upper()


def test_cadunico_and_analise_snh(
    selections: dict[str, tuple[list[Any], list[Any]]],
) -> None:
    pessoa, _ = selections["cadunico_pessoa_pbf"]
    pf, _ = selections["analise_snh_tab_validacao_pf"]

    assert [r.relative.split("/")[0] for r in pessoa] == ["CadUnico", "CadUnico"]
    assert pf and all("tab_validacao_arquivos_pf" in r.name for r in pf)


def test_positional_pii_only_for_the_headerless_alienation_file(
    specs: dict[str, Any],
) -> None:
    masks = [spec.prepare[1] for spec in specs.values()]
    mask = masks[0]

    assert all(m == mask for m in masks)
    assert dict(mask.positions) == {
        r"^CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112\.TXT$": {2: "cpf", 3: "nis"}
    }


def test_tasks_read_the_variable_and_only_call_the_pipeline_steps(
    module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = record_steps(module, monkeypatch)
    dag = module.dag_instance

    run_task(dag, "convert_to_staging", run_task(dag, "extract_to_raw"))

    assert FakeVariable.reads == ["SFTP_FAMILIAS", "SFTP_FAMILIAS"]
    assert calls == [
        ("extract_to_raw", module.FAMILIA, RUN_AFTER, FAMILIAS),
        ("convert_to_staging", module.FAMILIA, "raw/x/", FAMILIAS),
    ]


def test_nothing_new_is_a_skip(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    nothing_new(module, monkeypatch)

    with pytest.raises(AirflowSkipException):
        run_task(module.dag_instance, "extract_to_raw")
