"""Preparo `MaskPii`: PII mascarada antes do pouso, igual ao mascarar_minio.

As saídas `.esperado` foram geradas pelo `scripts/mascarar_minio.py` com o segredo
de teste: o preparo tem de reproduzi-las byte a byte (mesmos tokens HMAC, mesma
redação, mesmo CSV reescrito), para o dado novo continuar juntando com o antigo.
"""

import dataclasses
import shutil
import zipfile
from pathlib import Path

import pytest

from ingestion.extractors import RawFile, describe_file
from ingestion.prepare import MaskPii

FIXTURES = Path(__file__).parent / "fixtures" / "masking"
SECRET = "segredo-de-teste"


@pytest.fixture(autouse=True)
def secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASKING_HMAC_SECRET", SECRET)


def _mask(name: str, tmp_path: Path, step: MaskPii | None = None) -> list[RawFile]:
    source = tmp_path / "in" / name
    source.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURES / name, source)
    part = dataclasses.replace(describe_file(source), source_id="origem:1:2")
    return list((step or MaskPii()).apply(part, tmp_path / "work"))


@pytest.mark.parametrize("name", ["pf.csv"])
def test_tabular_matches_the_legacy_masking_byte_by_byte(
    name: str, tmp_path: Path
) -> None:
    [out] = _mask(name, tmp_path)

    assert out.name == name
    assert out.path.read_bytes() == (FIXTURES / f"{name}.esperado").read_bytes()
    assert out.source_id == "origem:1:2"
    masking = out.details["masking"]
    assert masking["status"] == "mascarado"  # type: ignore[index]
    assert masking["columns"] == [  # type: ignore[index]
        {"column": "NU_CPF", "category": "cpf", "action": "hmac"},
        {"column": "NO_PESSOA", "category": "nome", "action": "redact"},
        {"column": "DT_NASCIMENTO", "category": "nascimento", "action": "redact"},
        {"column": "NU_CEP", "category": "cep", "action": "redact"},
    ]
    assert (masking["rows"], masking["changed"]) == (3, 2)  # type: ignore[index]


def test_file_without_header_uses_declared_positions(tmp_path: Path) -> None:
    name = "CAIXA_AF_GEHIS_ALIENACAO_IMOVEL_M202112.TXT"
    step = MaskPii(positions={r"ALIENACAO_IMOVEL_M202112\.TXT$": {2: "cpf", 3: "nis"}})

    [out] = _mask(name, tmp_path, step)

    assert out.path.read_bytes() == (FIXTURES / f"{name}.esperado").read_bytes()


def test_xlsx_matches_the_legacy_masking(tmp_path: Path) -> None:
    [out] = _mask("pf.xlsx", tmp_path)

    with (
        zipfile.ZipFile(out.path) as got,
        zipfile.ZipFile(FIXTURES / "pf.xlsx.esperado") as want,
    ):
        assert got.namelist() == want.namelist()
        for entry in want.namelist():
            assert got.read(entry) == want.read(entry), entry


def test_file_without_pii_passes_unchanged(tmp_path: Path) -> None:
    [out] = _mask("pj.txt", tmp_path)

    assert out.path.read_bytes() == (FIXTURES / "pj.txt").read_bytes()
    assert out.details["masking"] == {"status": "sem_pii"}


def test_unsupported_format_passes_and_is_recorded(tmp_path: Path) -> None:
    source = tmp_path / "in" / "ementario.xls"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"xls")

    [out] = list(MaskPii().apply(describe_file(source), tmp_path / "work"))

    assert out.path.read_bytes() == b"xls"
    assert out.details["masking"] == {"status": "nao_suportado"}


def test_without_secret_masking_refuses_to_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MASKING_HMAC_SECRET")

    with pytest.raises(ValueError, match="MASKING_HMAC_SECRET"):
        _mask("pf.csv", tmp_path)


def test_row_with_more_fields_than_the_header_is_fully_redacted(tmp_path: Path) -> None:
    # Na INT039 de 2026-04-30, 2 linhas têm 40 campos num layout de 37: a posição
    # das colunas não vale, e o CEP caía numa coluna que não é mascarada.
    source = tmp_path / "INT039_TESTE.TXT"
    source.write_bytes(
        b"NU_CPF|NO_MUTUARIO|CO_CEP|NO_MUNICIPIO\n"
        b"12345678901|JOAO|37550000|POUSO ALEGRE\n"
        b"98765432100|MARIA|extra|37550001|POUSO ALEGRE\n"
    )
    [part] = MaskPii().apply(describe_file(source), tmp_path / "work")

    good, shifted = part.path.read_bytes().decode("latin-1").splitlines()[1:]
    assert good.split("|")[1:3] == ["***", "***"]
    assert shifted == "***|***|***|***|***"
