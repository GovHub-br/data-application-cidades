"""Template Method da ingestão do data lake do conjuntura (Etapa 02).

Pipeline do conjuntura contínuo:
    Etapa 01 (RAW)     CSV / TXT / XLSX / API-JSON  -> raw/<fonte>/<dado>.<ext>
    Etapa 02 (STAGING) TODOS convertidos p/ parquet -> staging/<fonte>/<dado>.parquet
    Etapa 03 (SILVER)  pg_duckdb read_parquet        -> Postgres (dbt)

Este módulo implementa a Etapa 02 com o padrão Template Method: o esqueleto
(baixar raw -> DataFrame -> validar schema -> texto -> escrever parquet) é
invariante; o que varia é só o *parse por formato* (csv/txt/xlsx via
ParserFactory, json via hook próprio).

A staging sai com TODAS as colunas como texto — fiel ao valor que a fonte
mandou, sem inferência de tipo do pandas pelo meio. A tipagem real (numeric,
date...) é responsabilidade do dbt (camada silver), via macros de parse. Por
isso a leitura de CSV/XLSX força `dtype=str`: se deixar o pandas inferir tipo
na leitura e só castar pra string depois, o valor já saiu reformatado (zero à
esquerda sumindo, notação científica, "1234.0" em coluna com nulo) antes do
cast conseguir "desfazer" — daí a staging teria que texto fiel, e não.

A validação de schema (`dataset_config`) é estrutural e opcional: só confere
se as colunas esperadas estão presentes. Nunca valida tipo/valor — isso é
sempre dbt.

`BaseFileParser` cobre fontes CSV/TXT/XLSX/JSON simples direto (fonte, dado,
formato) — nenhuma subclasse é necessária. Só crie uma subclasse quando o
arquivo tiver alguma particularidade estrutural (header/sheet_name fora do
padrão via `_read_kwargs`, ou JSON aninhado via `_json_para_dataframe`); a
tipagem em si nunca é motivo pra subclassear — isso é sempre responsabilidade
do dbt.
"""

import json
import logging
from typing import Any

import pandas as pd

from cliente_minio import download_raw_bytes, upload_raw_bytes
from domain.models import DatasetConfig, ParseResult, SourceFile
from parquet_writer import salvar_staging_parquet
from parser_factory import ParserFactory
from schema_validator import SchemaValidator


def registros_para_staging_parquet(
    fonte: str,
    dado: str,
    registros: list[dict[str, Any]],
    dataset_config: DatasetConfig | None = None,
) -> ParseResult | None:
    """Etapa 02 para fontes API-JSON com `registros` já normalizados pelo cliente.

    Monta o DataFrame a partir dos registros e escreve o parquet (texto) na
    staging. O cliente não deve mais tipar os valores antes de chamar isto —
    só repassar o que a API respondeu.
    """
    if not registros:
        logging.warning(f"[base_file_parser] Sem registros p/ parquet: {fonte}.{dado}")
        return None

    df = pd.DataFrame(registros)
    if dataset_config:
        SchemaValidator(dataset_config.expected_columns).validate(df)

    source = SourceFile(fonte=fonte, dado=dado, formato="json")
    return salvar_staging_parquet(source, df)


class BaseFileParser:
    """Template Method: leva um dado do RAW ao parquet (texto) da STAGING."""

    #: separador default para formato "txt"
    _sep: str = "\t"

    def __init__(
        self,
        fonte: str,
        dado: str,
        formato: str,
        dataset_config: DatasetConfig | None = None,
    ) -> None:
        self.fonte = fonte
        self.dado = dado
        self.formato = formato  # "csv" | "txt" | "xlsx" | "json"
        self.dataset_config = dataset_config

    # ------------------------- TEMPLATE METHOD (invariante) -------------------
    def gerar_staging_parquet(self) -> ParseResult:
        """Executa a Etapa 02: raw -> DataFrame -> valida schema -> parquet staging."""
        raw = download_raw_bytes(self.fonte, self.dado, ext=self.formato)
        df = self._para_dataframe(raw)
        if self.dataset_config:
            SchemaValidator(self.dataset_config.expected_columns).validate(df)
        source = SourceFile(fonte=self.fonte, dado=self.dado, formato=self.formato)
        return salvar_staging_parquet(source, df)

    # ------------------------- parse por FORMATO -------------------------------
    def _para_dataframe(self, raw: bytes) -> pd.DataFrame:
        if self.formato == "json":
            return self._json_para_dataframe(json.loads(raw.decode("utf-8")))
        parser = ParserFactory.create(self.formato)
        kwargs = self._read_kwargs()
        if self.formato == "txt":
            kwargs.setdefault("sep", self._sep)
        return parser.read(raw, **kwargs)

    # ------------------------- hooks com default (override se preciso) --------
    def _read_kwargs(self) -> dict[str, Any]:
        """kwargs extras p/ pandas.read_csv/read_excel (header, sheet_name...)."""
        return {}

    def _json_para_dataframe(self, obj: Any) -> pd.DataFrame:
        """Default: json já 'flat' -> DataFrame. Override p/ json aninhado."""
        return pd.DataFrame(obj)

    # ------------------------- infra (invariante) ----------------------------
    def subir_raw(
        self, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Conveniência da Etapa 01: sobe o arquivo cru recebido para a raw."""
        upload_raw_bytes(
            self.fonte, self.dado, data, ext=self.formato, content_type=content_type
        )
