"""fresh_import never lets a real environment reach the code under test."""

from tests.conftest import fresh_import


def test_real_env_does_not_leak(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "real.invalid:9000")
    monkeypatch.setenv("DB_DW_HOST_MCID", "real.invalid")
    config = fresh_import(monkeypatch, "mascaramento.config")
    assert (config.MINIO_ENDPOINT, config.PG_HOST) == ("minio:9000", "postgres")
