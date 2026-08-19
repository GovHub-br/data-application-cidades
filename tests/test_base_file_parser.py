from typing import Any

import pandas as pd
import pytest

import base_file_parser
import parquet_writer
from base_file_parser import BaseFileParser, registros_para_staging_parquet
from domain.exceptions import SchemaValidationError
from domain.models import DatasetConfig


class FakeParser(BaseFileParser):
    """Registra a ordem das etapas chamadas — só o Template Method, sem I/O real."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.steps: list[str] = []

    def _para_dataframe(self, raw: bytes) -> pd.DataFrame:
        self.steps.append("_para_dataframe")
        return pd.DataFrame({"id": ["1", "2"]})


@pytest.fixture(autouse=True)
def _sem_minio(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nenhum teste deste arquivo deve tocar o MinIO de verdade."""
    monkeypatch.setattr(
        base_file_parser, "download_raw_bytes", lambda fonte, dado, ext: b"raw"
    )
    monkeypatch.setattr(
        parquet_writer, "upload_staging_parquet", lambda fonte, dado, data: None
    )


def test_template_method_chama_para_dataframe_e_salva(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    salvos: list[pd.DataFrame] = []
    monkeypatch.setattr(
        parquet_writer,
        "upload_staging_parquet",
        lambda fonte, dado, data: salvos.append(data),
    )

    parser = FakeParser(fonte="teste", dado="dado", formato="csv")
    resultado = parser.gerar_staging_parquet()

    assert parser.steps == ["_para_dataframe"]
    assert resultado.rows == 2
    assert resultado.columns == 1
    assert len(salvos) == 1


def test_sem_dataset_config_nao_valida_schema() -> None:
    parser = FakeParser(fonte="teste", dado="dado", formato="csv")
    resultado = parser.gerar_staging_parquet()
    assert resultado.rows == 2


def test_dataset_config_com_schema_valido_passa() -> None:
    parser = FakeParser(
        fonte="teste",
        dado="dado",
        formato="csv",
        dataset_config=DatasetConfig(name="teste", expected_columns=["id"]),
    )
    resultado = parser.gerar_staging_parquet()
    assert resultado.rows == 2


def test_dataset_config_com_schema_invalido_levanta_erro() -> None:
    parser = FakeParser(
        fonte="teste",
        dado="dado",
        formato="csv",
        dataset_config=DatasetConfig(
            name="teste", expected_columns=["id", "coluna_que_nao_existe"]
        ),
    )
    with pytest.raises(SchemaValidationError):
        parser.gerar_staging_parquet()


def test_registros_para_staging_parquet_sem_registros_retorna_none() -> None:
    assert registros_para_staging_parquet("fonte", "dado", []) is None


def test_registros_para_staging_parquet_com_registros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    salvos: list[bytes] = []
    monkeypatch.setattr(
        parquet_writer,
        "upload_staging_parquet",
        lambda fonte, dado, data: salvos.append(data),
    )

    resultado = registros_para_staging_parquet(
        "fonte", "dado", [{"id": 1, "valor": "1891.63"}]
    )

    assert resultado is not None
    assert resultado.rows == 1
    assert len(salvos) == 1


def test_registros_com_dataset_config_invalido_levanta_erro() -> None:
    with pytest.raises(SchemaValidationError):
        registros_para_staging_parquet(
            "fonte",
            "dado",
            [{"id": 1}],
            dataset_config=DatasetConfig(
                name="teste", expected_columns=["id", "faltando"]
            ),
        )
