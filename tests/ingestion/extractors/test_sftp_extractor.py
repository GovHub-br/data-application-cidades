"""Estratégia `sftp`: arquivos de uma pasta remota, por padrão, em janelas."""

import os
from pathlib import Path
from typing import Any

import pytest

from ingestion.extractors import (
    ExtractionError,
    ExtractorConfig,
    ExtractorFactory,
    RemoteFiles,
)
from ingestion.extractors.models import sftp_extractor
from tests.ingestion.extractors.conftest import WHEN


class _Attr:
    def __init__(self, path: Path) -> None:
        info = path.stat()
        self.filename = path.name
        self.st_size = info.st_size
        self.st_mtime = int(info.st_mtime)
        self.st_mode = info.st_mode


class _RemoteFile:
    def __init__(self, path: Path, reads: list[tuple[int, int]]) -> None:
        self._data = path.read_bytes()
        self._reads = reads

    def readv(self, chunks: list[tuple[int, int]]) -> list[bytes]:
        self._reads.extend(chunks)
        return [self._data[offset : offset + size] for offset, size in chunks]

    def __enter__(self) -> "_RemoteFile":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


class _Client:
    """SFTPClient falso sobre uma pasta local."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.reads: list[tuple[int, int]] = []

    def _local(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def listdir_attr(self, path: str) -> list[_Attr]:
        return [_Attr(p) for p in sorted(self._local(path).iterdir())]

    def open(self, path: str, mode: str = "rb") -> _RemoteFile:
        return _RemoteFile(self._local(path), self.reads)


@pytest.fixture
def server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Client:
    root = tmp_path / "sftp"
    client = _Client(root)

    class Hook:
        def __init__(self, ssh_conn_id: str) -> None:
            assert ssh_conn_id == "sftp_teste"

        def get_conn(self) -> _Client:
            return client

        def close_conn(self) -> None:
            pass

    monkeypatch.setattr(sftp_extractor, "SFTPHook", Hook)
    files = {
        "fabrica/GEFUS/INT055_LIBERACOES_20260801.txt": b"a" * 10,
        "fabrica/GEFUS/ANTERIORES/INT055_LIBERACOES_20260701.txt": b"b" * 5,
        "fabrica/GEFUS/202608_SNH_AF_BB.csv": b"csv",
        "fabrica/GEFUS/202608_SNH_AF_BB.xlsx": b"xlsx",
        "fabrica/GEFUS/202607_SNH_AF_BB.xlsx": b"so xlsx",
        "fabrica/GEFUS/~$temporario.xlsx": b"lixo",
    }
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    os.utime(
        root / "fabrica/GEFUS/INT055_LIBERACOES_20260801.txt",
        (1_700_000_000, 1_700_000_000),
    )
    return client


def _extract(
    tmp_path: Path, files: RemoteFiles, landed: frozenset[str] = frozenset()
) -> list[Any]:
    config = ExtractorConfig(source="sftp", conn_id="sftp_teste", remote=files)
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    extractor.already_landed = landed
    return list(extractor.extract(tmp_path / "work"))


def test_matches_the_pattern_recursively_and_identifies_by_name_size_date(
    server: _Client, tmp_path: Path
) -> None:
    parts = _extract(
        tmp_path,
        RemoteFiles(root="/fabrica/GEFUS", pattern=r"INT055_LIBERACOES_\d{8}\.txt$"),
    )

    assert [(p.name, p.path.read_bytes()) for p in parts] == [
        ("INT055_LIBERACOES_20260701.txt", b"b" * 5),
        ("INT055_LIBERACOES_20260801.txt", b"a" * 10),
    ]
    assert parts[1].source_id == "INT055_LIBERACOES_20260801.txt:10:1700000000"


def test_already_landed_files_are_skipped(server: _Client, tmp_path: Path) -> None:
    files = RemoteFiles(root="/fabrica/GEFUS", pattern=r"INT055_.*\.txt$")

    parts = _extract(
        tmp_path, files, frozenset({"INT055_LIBERACOES_20260801.txt:10:1700000000"})
    )

    assert [p.name for p in parts] == ["INT055_LIBERACOES_20260701.txt"]


def test_one_delivery_per_stem_by_extension_precedence(
    server: _Client, tmp_path: Path
) -> None:
    parts = _extract(
        tmp_path,
        RemoteFiles(
            root="/fabrica/GEFUS",
            pattern=r"_SNH_AF_BB\.(csv|xlsx)$",
            prefer_extensions=(".csv", ".txt", ".xlsx"),
        ),
    )

    assert [p.name for p in parts] == ["202607_SNH_AF_BB.xlsx", "202608_SNH_AF_BB.csv"]


def test_the_same_delivery_in_other_folders_and_wrappers_lands_once(
    server: _Client, tmp_path: Path
) -> None:
    # Como no raw_para_staging: a identidade é o nome sem pasta e sem as extensões
    # da lista, encadeadas (X.TXT, X.TXT.zip e X.zip são a mesma entrega).
    files = {
        "GEFUS/INT059_FDS_20231031.TXT": 1_700_000_000,
        "GEFUS/ANTERIORES/INT059_FDS_20231031.TXT.zip": 1_700_000_100,
        "GEFUS/ANTERIORES/INT059_FDS_20231031.zip": 1_700_000_100,
        "GEFUS/ANTERIORES/INT059_FDS_20231130.zip": 1_700_000_000,
        "GEFUS/ANTERIORES/INT059_FDS_20231130.TXT.zip": 1_700_000_100,
    }
    for name, mtime in files.items():
        path = server.root / "fabrica" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        os.utime(path, (mtime, mtime))

    parts = _extract(
        tmp_path,
        RemoteFiles(
            root="/fabrica/GEFUS",
            pattern=r"INT059_",
            prefer_extensions=(".csv", ".txt", ".xlsx", ".zip"),
        ),
    )

    assert [p.name for p in parts] == [
        "INT059_FDS_20231130.TXT.zip",  # empate na extensão: a mais nova
        "INT059_FDS_20231031.TXT",
    ]


def test_bundles_come_after_the_loose_deliveries(server: _Client, tmp_path: Path) -> None:
    # Pacote mensal com várias famílias: entra também, mas depois das entregas
    # soltas, para que a solta fique com o nome quando as duas trazem o mesmo.
    for name in ("GEFUS/ANTERIORES/202312_CAIXA.zip", "GEFUS/ANTERIORES/202311.zip"):
        (server.root / "fabrica" / name).write_bytes(b"zip")

    parts = _extract(
        tmp_path,
        RemoteFiles(
            root="/fabrica/GEFUS",
            pattern=r"INT055_",
            bundles=r"(^|/)\d{6}(_CAIXA)?\.zip$",
            prefer_extensions=(".txt", ".zip"),
        ),
    )

    assert [p.name for p in parts] == [
        "INT055_LIBERACOES_20260701.txt",
        "INT055_LIBERACOES_20260801.txt",
        "202311.zip",
        "202312_CAIXA.zip",
    ]


def test_exclude_and_non_recursive(server: _Client, tmp_path: Path) -> None:
    parts = _extract(
        tmp_path,
        RemoteFiles(
            root="/fabrica/GEFUS",
            pattern=r"\.(txt|xlsx)$",
            recursive=False,
            exclude=(r"(^|/)~\$",),
        ),
    )

    assert [p.name for p in parts] == [
        "202607_SNH_AF_BB.xlsx",
        "202608_SNH_AF_BB.xlsx",
        "INT055_LIBERACOES_20260801.txt",
    ]


def test_download_reads_in_windows(
    server: _Client, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sftp_extractor, "WINDOW_BYTES", 4)

    _extract(tmp_path, RemoteFiles(root="/fabrica/GEFUS", pattern=r"20260801\.txt$"))

    assert server.reads == [(0, 4), (4, 4), (8, 2)]


def test_nothing_matching_is_an_extraction_error(server: _Client, tmp_path: Path) -> None:
    with pytest.raises(ExtractionError, match="nenhum arquivo"):
        _extract(tmp_path, RemoteFiles(root="/fabrica/GEFUS", pattern=r"nada"))


def test_config_without_remote_is_rejected() -> None:
    with pytest.raises(ValueError, match="remote"):
        ExtractorFactory.create(
            ExtractorConfig(source="sftp", conn_id="sftp_teste"), ingestion_time=WHEN
        )
