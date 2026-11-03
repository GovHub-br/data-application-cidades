"""Regressões do controle de versão da ingestão SFTP."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def modulo(monkeypatch):
    """Carrega o script sem abrir rede, com a configuração mínima exigida."""
    for chave, valor in {
        "SFTP_HOST": "example.invalid",
        "SFTP_USER": "teste",
        "SFTP_PASSWORD": "teste",
        "MINIO_ENDPOINT": "example.invalid:9000",
        "MINIO_ACCESS_KEY": "teste",
        "MINIO_SECRET_KEY": "teste",
        "MINIO_BUCKET": "teste",
        "DB_DW_HOST_MCID": "example.invalid",
        "DB_DW_USER_MCID": "teste",
        "DB_DW_PASSWORD_MCID": "teste",
        "DB_DW_DBNAME_MCID": "teste",
        "LAKE_LOCAL_ARTIFACTS": "0",
    }.items():
        monkeypatch.setenv(chave, valor)

    caminho = Path(__file__).parents[1] / "scripts" / "sftp_para_minio.py"
    spec = importlib.util.spec_from_file_location("sftp_para_minio_test", caminho)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_mesma_versao_exige_mtime_e_tamanho(monkeypatch) -> None:
    script = modulo(monkeypatch)
    inseridos = {"/home/fabrica/GEFUS/RCB.xlsx": (1_793_000_000, 1234)}

    assert script._mesma_versao(inseridos, "/home/fabrica/GEFUS/RCB.xlsx", 1_793_000_000, 1234)
    assert not script._mesma_versao(inseridos, "/home/fabrica/GEFUS/RCB.xlsx", 1_793_000_001, 1234)
    assert not script._mesma_versao(inseridos, "/home/fabrica/GEFUS/RCB.xlsx", 1_793_000_000, 1235)
    # Logs antigos sem metadados devem ser reprocessados uma vez, não tratados
    # como versão válida para sempre.
    assert not script._mesma_versao({"x": (None, None)}, "x", 1, 1)
