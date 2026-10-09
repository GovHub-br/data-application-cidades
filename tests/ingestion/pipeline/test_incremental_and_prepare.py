"""Extração incremental (o que já pousou não volta) e preparos antes do pouso."""

import dataclasses
import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.dataset import DatasetSpec
from ingestion.extractors import (
    Extractor,
    ExtractorConfig,
    ExtractorFactory,
    RawFile,
    write_stream,
)
from ingestion.pipeline import steps
from ingestion.storage import SUCCESS_MARKER, StorageFactory

WHEN = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)
SOURCE: dict[str, bytes] = {}
SEEN_BY_EXTRACTOR: list[frozenset[str]] = []


@ExtractorFactory.register("teste_incremental")
class _Folder(Extractor):
    """Fonte de teste: um arquivo por item de SOURCE, com source_id = nome + tamanho."""

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        SEEN_BY_EXTRACTOR.append(self.already_landed)
        for name, body in sorted(SOURCE.items()):
            source_id = f"{name}:{len(body)}"
            if source_id in self.already_landed:
                continue
            part = write_stream([body], work_dir / name, name)
            yield dataclasses.replace(part, source_id=source_id)


class _Upper:
    """Preparo de teste: troca o arquivo por uma versão em maiúsculas."""

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        body = part.path.read_bytes().upper()
        part.path.unlink()
        new = write_stream([body], work_dir / f"up_{part.name}", f"up_{part.name}")
        yield dataclasses.replace(
            new, source_id=part.source_id, details={**part.details, "upper": True}
        )


class _Split:
    """Preparo de teste: um arquivo vira dois (como um zip com dois membros)."""

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        for n in (1, 2):
            name = f"{n}_{part.name}"
            new = write_stream([part.path.read_bytes()], work_dir / name, name)
            yield dataclasses.replace(new, source_id=part.source_id, details=part.details)
        part.path.unlink()


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lake"
    monkeypatch.setenv("INGESTION_STORAGE_BACKEND", "local")
    monkeypatch.setenv("INGESTION_LOCAL_ROOT", str(root))
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path / "tmp"))
    SOURCE.clear()
    SEEN_BY_EXTRACTOR.clear()
    return root


def _manifest(lake: Path, prefix: str) -> list[dict[str, object]]:
    files: list[dict[str, object]] = json.loads(
        (lake / prefix / SUCCESS_MARKER).read_text()
    )["files"]
    return files


def _spec(**kwargs: object) -> DatasetSpec:
    return DatasetSpec(
        domain="sftp",
        dataset="teste",
        extractor=ExtractorConfig(source="teste_incremental"),
        **kwargs,
    )


def test_source_id_goes_to_the_manifest(lake: Path) -> None:
    SOURCE["a.csv"] = b"x"

    prefix = steps.extract_to_raw(_spec(), WHEN)

    assert _manifest(lake, prefix)[0]["source_id"] == "a.csv:1"


def test_incremental_run_lands_only_what_is_new(lake: Path) -> None:
    spec = _spec(incremental=True)
    SOURCE.update({"a.csv": b"x", "b.csv": b"y"})
    first = steps.extract_to_raw(spec, WHEN)
    SOURCE["c.csv"] = b"z"

    second = steps.extract_to_raw(spec, WHEN + timedelta(days=1))

    assert SEEN_BY_EXTRACTOR == [frozenset(), frozenset({"a.csv:1", "b.csv:1"})]
    storage = StorageFactory.create("local", root=lake)
    assert storage.list(second) == [second + SUCCESS_MARKER, second + "c.csv"]
    assert first != second
    with pytest.raises(AirflowSkipException):
        steps.extract_to_raw(spec, WHEN + timedelta(days=2))


def test_without_incremental_everything_lands_again(lake: Path) -> None:
    SOURCE["a.csv"] = b"x"
    steps.extract_to_raw(_spec(), WHEN)

    steps.extract_to_raw(_spec(), WHEN + timedelta(days=1))

    assert SEEN_BY_EXTRACTOR == [frozenset(), frozenset()]


def test_prepare_steps_run_in_order_before_landing(lake: Path, tmp_path: Path) -> None:
    SOURCE["a.csv"] = b"abc"

    prefix = steps.extract_to_raw(_spec(prepare=(_Split(), _Upper())), WHEN)

    storage = StorageFactory.create("local", root=lake)
    assert storage.list(prefix) == [
        prefix + SUCCESS_MARKER,
        prefix + "up_1_a.csv",
        prefix + "up_2_a.csv",
    ]
    assert (lake / prefix / "up_1_a.csv").read_bytes() == b"ABC"
    manifest = _manifest(lake, prefix)
    assert {(m["source_id"], m["upper"]) for m in manifest} == {("a.csv:3", True)}
    assert not any(p.is_file() for p in (tmp_path / "tmp").rglob("*"))


class _OnlyB:
    """Preparo de teste: descarta tudo que não é b (o pacote sem a família)."""

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        if part.name.startswith("b"):
            yield part
        else:
            part.path.unlink()


def test_a_source_that_the_prepare_drops_is_not_downloaded_again(lake: Path) -> None:
    spec = _spec(incremental=True, prepare=(_OnlyB(),))
    SOURCE.update({"a.zip": b"x", "b.csv": b"y"})
    prefix = steps.extract_to_raw(spec, WHEN)
    SOURCE["b2.csv"] = b"z"

    steps.extract_to_raw(spec, WHEN + timedelta(days=1))

    sources = json.loads((lake / prefix / SUCCESS_MARKER).read_text())["sources"]
    assert sources == ["a.zip:1", "b.csv:1"]
    assert SEEN_BY_EXTRACTOR[1] == frozenset({"a.zip:1", "b.csv:1"})


class _Unwrap:
    """Preparo de teste: `pacote_*` vira o arquivo que ele embrulha (b.csv)."""

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        if not part.name.startswith("pacote_"):
            yield part
            return
        new = write_stream(
            [part.path.read_bytes()], work_dir / "inner" / "b.csv", "b.csv"
        )
        part.path.unlink()
        yield dataclasses.replace(new, source_id=part.source_id)


def test_the_same_name_twice_after_the_prepare_keeps_the_first(lake: Path) -> None:
    # A entrega solta vem antes do pacote que também a traz.
    SOURCE.update({"b.csv": b"solta", "pacote_x": b"do pacote"})

    prefix = steps.extract_to_raw(_spec(prepare=(_Unwrap(),)), WHEN)

    assert (lake / prefix / "b.csv").read_bytes() == b"solta"
    marker = json.loads((lake / prefix / SUCCESS_MARKER).read_text())
    assert marker["duplicates"] == [{"name": "b.csv", "source_id": "pacote_x:9"}]
    assert marker["sources"] == ["b.csv:5", "pacote_x:9"]
