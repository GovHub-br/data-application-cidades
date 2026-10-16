"""Extração incremental (o que já pousou não volta), preparos antes do pouso e
várias entradas numa DAG."""

import dataclasses
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ingestion.dataset import DatasetSpec
from ingestion.extractors import (
    HttpRequest,
    Extractor,
    ExtractorConfig,
    ExtractorFactory,
    RawFile,
    write_stream,
    SourceNotFoundError,
)
from ingestion.pipeline import NothingNew, steps

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


class _MemoryLog:
    """Controle de teste: guarda em memória o que pousou, como a tabela do Postgres."""

    def __init__(self) -> None:
        self.rows: list[tuple[str | None, str]] = []

    def landed(self) -> frozenset[str]:
        return frozenset(source for source, _key in self.rows if source)

    def record(self, part: RawFile, key: str) -> None:
        self.rows.append((part.source_id, key))


def _spec(**kwargs: object) -> DatasetSpec:
    return DatasetSpec(
        domain="teste",
        dataset="teste",
        extractor=ExtractorConfig(source="teste_incremental"),
        **kwargs,
    )


def test_incremental_run_lands_only_what_is_new_and_records_it(lake: Path) -> None:
    log = _MemoryLog()
    SOURCE.update({"a.csv": b"1", "b.csv": b"22"})
    assert steps.extract_to_raw(_spec(incremental=log), WHEN) == {
        "teste": ["raw/teste/a.csv", "raw/teste/b.csv"]
    }
    SOURCE["c.csv"] = b"333"

    landed = steps.extract_to_raw(_spec(incremental=log), WHEN + timedelta(days=1))

    assert landed == {"teste": ["raw/teste/c.csv"]}
    assert SEEN_BY_EXTRACTOR[1] == frozenset({"a.csv:1", "b.csv:2"})
    assert log.rows == [
        ("a.csv:1", "raw/teste/a.csv"),
        ("b.csv:2", "raw/teste/b.csv"),
        ("c.csv:3", "raw/teste/c.csv"),
    ]


def test_incremental_run_with_nothing_new_is_nothing_new(lake: Path) -> None:
    log = _MemoryLog()
    SOURCE["a.csv"] = b"1"
    steps.extract_to_raw(_spec(incremental=log), WHEN)

    with pytest.raises(NothingNew):
        steps.extract_to_raw(_spec(incremental=log), WHEN + timedelta(days=1))


def test_a_failed_upload_is_not_recorded(
    lake: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # O registro vem depois do upload: o que não subiu volta na próxima execução.
    log = _MemoryLog()
    SOURCE.update({"a.csv": b"1", "b.csv": b"2"})
    from ingestion.storage import LocalStorageBackend

    real = LocalStorageBackend.put_file

    def flaky(self: LocalStorageBackend, key: str, local_path: Path) -> None:
        if key.endswith("b.csv"):
            raise OSError("MinIO caiu")
        real(self, key, local_path)

    monkeypatch.setattr(LocalStorageBackend, "put_file", flaky)

    with pytest.raises(OSError):
        steps.extract_to_raw(_spec(incremental=log), WHEN)
    assert log.rows == [("a.csv:1", "raw/teste/a.csv")]


def test_without_incremental_everything_lands_again_over_the_old(lake: Path) -> None:
    SOURCE["a.csv"] = b"1"
    steps.extract_to_raw(_spec(), WHEN)
    SOURCE["a.csv"] = b"2"

    steps.extract_to_raw(_spec(), WHEN + timedelta(days=1))

    assert SEEN_BY_EXTRACTOR == [frozenset(), frozenset()]
    assert (lake / "raw/teste/a.csv").read_bytes() == b"2"


def test_prepare_steps_run_in_order_before_landing(lake: Path, tmp_path: Path) -> None:
    SOURCE["a.csv"] = b"abc"

    [first, second] = steps.extract_to_raw(_spec(prepare=(_Split(), _Upper())), WHEN)[
        "teste"
    ]

    assert (first, second) == ("raw/teste/up_1_a.csv", "raw/teste/up_2_a.csv")
    assert (lake / first).read_bytes() == b"ABC"
    assert not any(p.is_file() for p in (tmp_path / "tmp").rglob("*"))


def test_the_same_name_twice_in_one_ingestion_is_an_error(lake: Path) -> None:
    SOURCE.update({"a.csv": b"1"})

    with pytest.raises(ValueError, match="repetido"):
        steps.extract_to_raw(_spec(prepare=(_Split(), _SameName())), WHEN)


class _SameName:
    """Preparo de teste: todo arquivo vira `x.csv`."""

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        new = write_stream(
            [part.path.read_bytes()], work_dir / "same" / part.name / "x.csv", "x.csv"
        )
        part.path.unlink()
        yield new


FILES: dict[str, dict[str, bytes]] = {}
EXTRACTED: list[str] = []


@ExtractorFactory.register("teste_entradas")
class _PerDataset(Extractor):
    """Fonte de teste: os arquivos de FILES[<request>]; "erro" quebra, "ausente" é 404."""

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        [request] = self.config.requests
        EXTRACTED.append(request.name)
        if request.name == "erro":
            raise RuntimeError("HTTP 500")
        if request.name == "ausente":
            raise SourceNotFoundError("404")
        for name, body in FILES.get(request.name, {}).items():
            yield write_stream([body], work_dir / name, name)


def _por_entrada(**kwargs: object) -> DatasetSpec:
    return DatasetSpec(
        domain="ibge",
        dataset=lambda entrada: entrada["dataset"],
        extractor=lambda entrada: ExtractorConfig(
            source="teste_entradas",
            requests=(HttpRequest(name=entrada["dataset"], endpoint="/"),),
        ),
        **kwargs,
    )


@pytest.fixture
def entradas(lake: Path) -> Iterator[None]:
    FILES.clear()
    EXTRACTED.clear()
    yield


def test_one_dataset_per_entry_in_order(lake: Path, entradas: None) -> None:
    FILES.update({"sinapi": {"s.json": b"[]"}, "paic": {"p.json": b"[]"}})

    landed = steps.extract_to_raw(
        _por_entrada(), WHEN, [{"dataset": "sinapi"}, {"dataset": "paic"}]
    )

    assert landed == {"sinapi": ["raw/ibge/s.json"], "paic": ["raw/ibge/p.json"]}


def test_the_first_error_stops_the_run(lake: Path, entradas: None) -> None:
    FILES["paic"] = {"p.json": b"[]"}

    with pytest.raises(RuntimeError, match="500"):
        steps.extract_to_raw(
            _por_entrada(), WHEN, [{"dataset": "erro"}, {"dataset": "paic"}]
        )
    assert EXTRACTED == ["erro"]


def test_a_dataset_without_data_is_not_an_error(lake: Path, entradas: None) -> None:
    FILES["paic"] = {"p.json": b"[]"}

    landed = steps.extract_to_raw(
        _por_entrada(), WHEN, [{"dataset": "ausente"}, {"dataset": "paic"}]
    )

    assert list(landed) == ["paic"]


def test_nothing_new_in_any_dataset(lake: Path, entradas: None) -> None:
    with pytest.raises(NothingNew):
        steps.extract_to_raw(_por_entrada(), WHEN, [{"dataset": "ausente"}])


def test_convert_only_the_datasets_that_landed(lake: Path, entradas: None) -> None:
    FILES["paic"] = {"p.json": b'[{"a": "1"}]'}
    lista = [{"dataset": "ausente"}, {"dataset": "paic"}]
    landed = steps.extract_to_raw(_por_entrada(), WHEN, lista)

    staged = steps.convert_to_staging(_por_entrada(), landed, lista)

    assert staged == {"paic": ["staging/ibge/p.parquet"]}


def test_the_list_must_have_unique_and_some_datasets(lake: Path, entradas: None) -> None:
    with pytest.raises(ValueError, match="repetido"):
        steps.extract_to_raw(_por_entrada(), WHEN, [{"dataset": "a"}, {"dataset": "a"}])
    with pytest.raises(ValueError, match="vazia"):
        steps.extract_to_raw(_por_entrada(), WHEN, [])
