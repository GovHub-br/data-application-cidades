"""Preparo `Unpack`: zip e gzip viram os arquivos de dentro, pelo conteúdo."""

import dataclasses
import gzip
import zipfile
from pathlib import Path

import pytest

from ingestion.extractors import RawFile, describe_file
from ingestion.prepare import Unpack


def _part(path: Path) -> RawFile:
    return dataclasses.replace(describe_file(path), source_id="origem:1:2")


def _apply(step: Unpack, path: Path, tmp_path: Path) -> list[tuple[str, bytes, RawFile]]:
    out = list(step.apply(_part(path), tmp_path / "work"))
    return [(p.name, p.path.read_bytes(), p) for p in out]


def test_zip_members_that_match_come_out_one_by_one(tmp_path: Path) -> None:
    archive = tmp_path / "Base_PF_FGTS_20260807.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("pasta/Base_PF_FGTS_20260807.txt", "a;b\n1;2\n")
        zf.writestr("leia-me.pdf", "pdf")

    out = _apply(Unpack(members=r"\.txt$"), archive, tmp_path)

    assert [(name, body) for name, body, _ in out] == [
        ("Base_PF_FGTS_20260807.txt", b"a;b\n1;2\n")
    ]
    part = out[0][2]
    assert part.source_id == "origem:1:2"
    assert part.details == {"archive": "Base_PF_FGTS_20260807.zip"}
    assert not archive.exists()


def test_gzip_named_zip_uses_the_name_in_its_header(tmp_path: Path) -> None:
    # Na fábrica, alguns ".zip" são gzip (CadÚnico, andamento de obra).
    archive = tmp_path / "CAIXA_ANDAMENTO_M20250710.TXT.zip"
    with open(archive, "wb") as raw:
        with gzip.GzipFile(
            filename="CAIXA_ANDAMENTO_M20250710.TXT", mode="wb", fileobj=raw
        ) as gz:
            gz.write(b"linha 1\nlinha 2\n")

    out = _apply(Unpack(), archive, tmp_path)

    assert [(name, body) for name, body, _ in out] == [
        ("CAIXA_ANDAMENTO_M20250710.TXT", b"linha 1\nlinha 2\n")
    ]


def test_gzip_without_name_drops_the_compression_suffix(tmp_path: Path) -> None:
    archive = tmp_path / "dados.txt.gz"
    with gzip.GzipFile(archive, "wb", mtime=0) as gz:
        gz.write(b"x")
    # GzipFile grava o nome do arquivo; regrava sem a flag FNAME
    data = bytearray(archive.read_bytes())
    data[3] &= ~0x08
    end = data.index(0, 10)
    archive.write_bytes(bytes(data[:10] + data[end + 1 :]))

    out = _apply(Unpack(), archive, tmp_path)

    assert [(name, body) for name, body, _ in out] == [("dados.txt", b"x")]


def test_other_files_pass_through_unchanged(tmp_path: Path) -> None:
    plain = tmp_path / "INT055_20260801.txt"
    plain.write_bytes(b"conteudo")

    out = _apply(Unpack(), plain, tmp_path)

    assert [(name, body) for name, body, _ in out] == [
        ("INT055_20260801.txt", b"conteudo")
    ]


def test_prefix_with_archive_keeps_repeated_member_names_apart(tmp_path: Path) -> None:
    archive = tmp_path / "MC20260821.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("MCidades_AO_1.mdb", "mdb")

    out = _apply(Unpack(prefix_with_archive=True), archive, tmp_path)

    assert [name for name, _, _ in out] == ["MC20260821__MCidades_AO_1.mdb"]


def test_zip_without_matching_member_is_an_error(tmp_path: Path) -> None:
    archive = tmp_path / "x.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("a.pdf", "pdf")

    with pytest.raises(ValueError, match="nenhum membro"):
        _apply(Unpack(members=r"\.txt$"), archive, tmp_path)
