"""Base of the new ingestion (plugins/datalake): load ids, contracts and structural drift."""

import re
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from datalake import contrato, leitores
from datalake.chaves import load_id, load_id_of, raw_key
from tests.conftest import fresh_import

DATASETS = contrato.load()


def test_load_id_is_utc_and_sortable():
    moment = datetime(2026, 11, 11, 0, 30, tzinfo=timezone(timedelta(hours=-3)))
    assert load_id(moment) == "20261111T033000Z"
    assert load_id(moment) < load_id(moment + timedelta(seconds=1))


def test_raw_key_puts_the_load_folder_before_the_file():
    key = raw_key("sftp/fabrica/GEFUS", "X.csv", "20261111T033000Z")
    assert key == "raw/sftp/fabrica/GEFUS/load_id=20261111T033000Z/X.csv"
    assert load_id_of(key) == "20261111T033000Z"
    assert load_id_of("raw/sftp/fabrica/GEFUS/X.csv") is None


@pytest.mark.parametrize(
    "key, expected",
    [
        (
            "raw/sftp/fabrica/GEFUS/load_id=20261111T030000Z/PMCMV_FAIXA3_MCID_2026_11_01.csv",
            ("sftp_gefus", "gefus_fundo_social"),
        ),
        (
            "raw/sftp/fabrica/GEFUS/CadUnico/load_id=20261111T030000Z/ARQ_PESSOA_PBF_01112026_SB8.TXT",
            ("cadunico", "cadunico_pessoa"),
        ),
        # the family cut arrives in different folders; one dataset covers both
        (
            "raw/sharepoint/Cadastro Único/load_id=20261111T030000Z/ARQ_FAMILIA_PBF_01112026_SB8.TXT",
            ("cadunico", "cadunico_familia"),
        ),
        # each Access table is its own dataset
        (
            "raw/sftp/caixa.geavo/GEAVO/load_id=20261111T030000Z/"
            "MC20261101__MCidades_CCI_CCA_2026.mdb/CCI_Analitico.csv",
            ("sftp_geavo", "geavo_cci_analitico"),
        ),
        ("raw/sftp/fabrica/GEFUS/load_id=20261111T030000Z/QUALQUER_COISA.csv", None),
    ],
)
def test_match_finds_the_dataset_of_a_delivery(key, expected):
    found = contrato.match(key, DATASETS)
    assert (found and (found.source, found.name)) == expected


def test_no_delivery_matches_two_datasets():
    keys = [(ds.folder, ds.file_pattern.pattern, ds.access_table) for ds in DATASETS]
    assert DATASETS and len(keys) == len(set(keys))


DS = contrato.Dataset(
    source="s",
    name="d",
    path="staging/s/D_*.parquet",
    file_pattern=re.compile(r"D_\d{8}\.csv"),
    max_row_variation=0.5,
    columns=("a", "b", "c"),
    aliases={"c": ("c_antiga",)},
)


@pytest.mark.parametrize(
    "header, rows, previous, status",
    [
        (["a", "b", "c"], None, None, "approved"),
        (["a", "b", "c", "novo"], None, None, "additive_drift"),
        (["a", "b", "c_antiga"], None, None, "known_rename"),
        (["a", "b"], None, None, "quarantined"),
        (["a", "b", "outra"], None, None, "quarantined"),
        (["a", "b", "c"], 40, 100, "quarantined"),  # a full load that lost 60% of the rows
        (["a", "b", "c"], 90, 100, "approved"),
    ],
)
def test_structural_status(header, rows, previous, status):
    assert contrato.structural_status(header, DS, rows, previous)[0] == status


def test_without_contract_is_uncataloged():
    assert contrato.structural_status(["a"], None)[0] == "uncataloged"


def test_unknown_reader_is_refused():
    with pytest.raises(KeyError, match="no reader"):
        leitores.convert(None, "raw/x.csv", "e", True, set(), reader_name="nao_existe")


def test_new_loads_get_load_id_and_legacy_keys_do_not(monkeypatch, tmp_path):
    proc = fresh_import(
        monkeypatch, "conversao.processamento", "conversao", LAKE_TMPDIR=str(tmp_path)
    )
    conteudo = b"ID;NOME\n1;Ana\n"

    class Lake:
        enviados = {}

        def sample_bytes(self, key):
            return conteudo

        def baixar_para_tempfile(self, key, ext, tmpdir):
            path = tmp_path / f"d{ext}"
            path.write_bytes(conteudo)
            return str(path)

        def upload_arquivo(self, path, key):
            Lake.enviados[key] = pq.read_table(path)

    nova = "raw/sftp/a/load_id=20261111T030000Z/D.csv"
    for key in (nova, "raw/sftp/a/D.csv"):
        proc.processar_objeto(Lake(), key, "e", True, set(), 1000, "skip")
    novo = Lake.enviados["staging/sftp/a/load_id=20261111T030000Z/D.parquet"]
    assert novo.column("_load_id").to_pylist() == ["20261111T030000Z"]
    assert "_load_id" not in Lake.enviados["staging/sftp/a/D.parquet"].column_names
