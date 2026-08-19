from dataclasses import dataclass


@dataclass
class SourceFile:
    """Identifica uma fonte no data lake (raw/staging no MinIO)."""

    fonte: str
    dado: str
    formato: str


@dataclass
class ParseResult:
    """Resultado de gerar o parquet (texto) de staging p/ uma fonte."""

    source: SourceFile
    rows: int
    columns: int


@dataclass
class DatasetConfig:
    """Config opcional de validação estrutural de um dataset (ver SchemaValidator).

    `expected_columns` valida só PRESENÇA de coluna — nunca tipo/valor. Tipagem
    e regra de negócio são responsabilidade do dbt (camada silver), não daqui.
    """

    name: str
    expected_columns: list[str]
    partition_columns: list[str] | None = None
