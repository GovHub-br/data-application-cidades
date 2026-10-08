"""Pouso na raw: sobe cada parte, apaga a cópia local, marca a ingestão completa."""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion.extractors import RawFile, write_stream
from ingestion.raw import SUCCESS_MARKER, RawLanding
from ingestion.storage import StorageBackend, StorageFactory

PREFIX = "raw/ibge/sinapi/2026-10-08/060000/"


@pytest.fixture(params=["local", "s3"])
def storage(request: pytest.FixtureRequest, tmp_path: Path) -> StorageBackend:
    if request.param == "local":
        return StorageFactory.create("local", root=tmp_path / "lake")
    backend: StorageBackend = request.getfixturevalue("s3_backend")
    return backend


def _parts(tmp_path: Path, contents: dict[str, bytes]) -> Iterator[RawFile]:
    for name, data in contents.items():
        yield write_stream([data], tmp_path / "work" / name)


def test_lands_every_part_under_the_prefix_and_marks_success(
    storage: StorageBackend, tmp_path: Path
) -> None:
    result = RawLanding(storage).land(
        _parts(tmp_path, {"a.json": b"[1]", "b.json": b"[2, 3]"}), PREFIX
    )

    assert result.keys == (PREFIX + "a.json", PREFIX + "b.json")
    assert result.total_bytes == 9
    assert storage.list(PREFIX) == [
        PREFIX + "_SUCCESS",
        PREFIX + "a.json",
        PREFIX + "b.json",
    ]
    out = tmp_path / "out.json"
    storage.get_file(PREFIX + "b.json", out)
    assert out.read_bytes() == b"[2, 3]"


def test_success_marker_is_a_manifest_of_the_parts(
    storage: StorageBackend, tmp_path: Path
) -> None:
    parts = list(_parts(tmp_path, {"a.json": b"[1]"}))
    RawLanding(storage).land(iter(parts), PREFIX)

    marker = tmp_path / "marker.json"
    storage.get_file(PREFIX + SUCCESS_MARKER, marker)

    assert json.loads(marker.read_text()) == {
        "files": [{"name": "a.json", "size": 3, "sha256": parts[0].sha256}]
    }


def test_each_local_part_is_deleted_before_the_next_is_extracted(
    storage: StorageBackend, tmp_path: Path
) -> None:
    produced: list[Path] = []

    def parts() -> Iterator[RawFile]:
        for name in ["a.csv", "b.csv", "c.csv"]:
            assert all(not path.exists() for path in produced)
            raw = write_stream([b"x" * 100], tmp_path / "work" / name)
            produced.append(raw.path)
            yield raw

    RawLanding(storage).land(parts(), PREFIX)

    assert all(not path.exists() for path in produced)


def test_marker_is_uploaded_last(tmp_path: Path) -> None:
    uploads: list[str] = []
    local = StorageFactory.create("local", root=tmp_path / "lake")

    class Spy(StorageBackend):
        def put_file(self, key: str, local_path: Path) -> None:
            uploads.append(key)
            local.put_file(key, local_path)

        def get_file(self, key: str, local_path: Path) -> None:
            local.get_file(key, local_path)

        def list(self, prefix: str) -> list[str]:
            return local.list(prefix)

        def delete(self, key: str) -> None:
            local.delete(key)

        def exists(self, key: str) -> bool:
            return local.exists(key)

    RawLanding(Spy()).land(_parts(tmp_path, {"a": b"1", "b": b"2"}), PREFIX)

    assert uploads == [PREFIX + "a", PREFIX + "b", PREFIX + "_SUCCESS"]


def test_failure_mid_extraction_leaves_no_success_marker(
    storage: StorageBackend, tmp_path: Path
) -> None:
    def parts() -> Iterator[RawFile]:
        yield write_stream([b"1"], tmp_path / "work" / "a.json")
        raise RuntimeError("a fonte caiu no meio")

    with pytest.raises(RuntimeError):
        RawLanding(storage).land(parts(), PREFIX)

    assert not storage.exists(PREFIX + SUCCESS_MARKER)


def test_no_parts_means_no_marker(storage: StorageBackend) -> None:
    result = RawLanding(storage).land(iter(()), PREFIX)

    assert result.keys == ()
    assert storage.list(PREFIX) == []


@pytest.mark.parametrize("names", [["a.json", "a.json"], ["_SUCCESS"]])
def test_repeated_or_reserved_names_are_rejected(
    storage: StorageBackend, tmp_path: Path, names: list[str]
) -> None:
    def parts() -> Iterator[RawFile]:
        for n, name in enumerate(names):
            yield write_stream([b"1"], tmp_path / "work" / str(n) / name)

    with pytest.raises(ValueError):
        RawLanding(storage).land(parts(), PREFIX)
    assert not storage.exists(PREFIX + SUCCESS_MARKER)


def test_prefix_must_be_a_partition_directory(storage: StorageBackend) -> None:
    with pytest.raises(ValueError):
        RawLanding(storage).land(iter(()), PREFIX.rstrip("/"))
