"""DAG do SFTP no pipeline novo: as famílias contra a árvore real da conta fábrica.

A fixture é a listagem do SFTP (caminho, tamanho, data) em 2026-10-09. Os testes
passam a seleção de cada dataset sobre ela: todo arquivo cai em uma família só,
ou é um avulso declarado aqui.
"""

import dataclasses
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from ingestion.extractors import RemoteFiles
from ingestion.extractors.models import sftp_extractor
from ingestion.loaders import LoadMode
from ingestion.prepare import MaskPii, Unpack
from tests.ingestion.dags.conftest import (
    RUN_AFTER,
    load_dag_module,
    record_steps,
    run_task,
)

ARVORE = Path(__file__).parent / "fixtures" / "sftp_fabrica_arvore.tsv"

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


@pytest.fixture
def module() -> Any:
    return load_dag_module("data_ingest/sftp/sftp_ingest_dag.py")


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
def selections(module: Any) -> dict[str, tuple[list[Any], list[Any]]]:
    return {spec.dataset: _selection(spec) for spec in module.DATASETS}


def test_dag_identity_and_one_task_group_per_family(module: Any) -> None:
    dag = module.dag_instance

    assert dag.dag_id == "sftp_ingest_dag"
    assert dag.schedule == "0 1 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.max_active_tasks == 4
    assert {"sftp", "ingestion"} <= set(dag.tags)
    for spec in module.DATASETS:
        convert = dag.task_dict[f"{spec.dataset}.convert_to_staging"]
        assert convert.upstream_task_ids == {f"{spec.dataset}.extract_to_raw"}


def test_every_family_is_incremental_overwrite_masked_and_auto_dialect(
    module: Any,
) -> None:
    for spec in module.DATASETS:
        config = spec.extractor_config()
        assert spec.domain == "sftp"
        assert config.source == "sftp" and config.conn_id == "sftp_mcid"
        assert spec.incremental is True
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
    module: Any, selections: dict[str, tuple[list[Any], list[Any]]]
) -> None:
    pacotes = {"202310.zip", "202311.zip", "202312_CAIXA.zip"}
    specs = {spec.dataset: spec for spec in module.DATASETS}

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


def test_positional_pii_only_for_the_headerless_alienation_file(module: Any) -> None:
    masks = [spec.prepare[1] for spec in module.DATASETS]
    mask = masks[0]

    assert all(m == mask for m in masks)
    assert dict(mask.positions) == {
        r"^CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112\.TXT$": {2: "cpf", 3: "nis"}
    }


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
