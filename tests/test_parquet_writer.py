import io
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import pytest

import parquet_writer
from domain.models import SourceFile
from parquet_writer import _stringificar, salvar_staging_parquet


def test_stringificar_cast_tudo_pra_texto_preserva_nulo() -> None:
    df = pd.DataFrame(
        {" Valor ": ["1891.63", None, "007"], "Data": ["01/2024", "02/2024", None]}
    )
    out = _stringificar(df)

    assert list(out.columns) == ["valor", "data"]
    assert out["valor"].tolist()[0] == "1891.63"
    assert out["valor"].tolist()[2] == "007"
    assert pd.isna(out["valor"].tolist()[1])


def test_salvar_staging_parquet_sobe_bytes_com_schema_100_por_cento_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capturado: dict[str, Any] = {}

    def fake_upload(fonte: str, dado: str, data: bytes) -> None:
        capturado["fonte"] = fonte
        capturado["dado"] = dado
        capturado["data"] = data

    monkeypatch.setattr(parquet_writer, "upload_staging_parquet", fake_upload)

    df = pd.DataFrame({"id": [1, 2], "valor": [1891.63, None]})
    source = SourceFile(fonte="ibge", dado="pib", formato="json")

    resultado = salvar_staging_parquet(source, df)

    assert resultado.rows == 2
    assert resultado.columns == 2
    assert capturado["fonte"] == "ibge"
    assert capturado["dado"] == "pib"

    schema = pq.read_schema(io.BytesIO(capturado["data"]))
    for tipo in schema.types:
        assert str(tipo) in ("string", "large_string")
