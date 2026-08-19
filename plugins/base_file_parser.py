"""Template Method da ingestão do data lake do conjuntura (Etapa 02).

Pipeline do conjuntura contínuo:
    Etapa 01 (RAW)     CSV / TXT / XLSX / API-JSON  -> raw/<fonte>/<dado>.<ext>
    Etapa 02 (STAGING) TODOS convertidos p/ parquet -> staging/<fonte>/<dado>.parquet
    Etapa 03 (SILVER)  pg_duckdb read_parquet        -> Postgres (dbt)

Este módulo implementa a Etapa 02 com o padrão Template Method: o esqueleto
(baixar raw -> DataFrame -> texto -> escrever parquet) é invariante; o que varia
é só o *parse por formato* (fechado em csv/txt/xlsx/json).

A staging sai com TODAS as colunas como texto — fiel ao valor que a fonte
mandou, sem inferência de tipo do pandas pelo meio. A tipagem real (numeric,
date...) é responsabilidade do dbt (camada silver), via macros de parse. Por
isso a leitura de CSV/XLSX força `dtype=str`: se deixar o pandas inferir tipo
na leitura e só castar pra string depois, o valor já saiu reformatado (zero à
esquerda sumindo, notação científica, "1234.0" em coluna com nulo) antes do
cast conseguir "desfazer" — daí a staging teria que texto fiel, e não.

`BaseFileParser` cobre fontes CSV/TXT/XLSX/JSON simples direto (fonte, dado,
formato) — nenhuma subclasse é necessária. Só crie uma subclasse quando o
arquivo tiver alguma particularidade estrutural (header/sheet_name fora do
padrão via `_read_kwargs`, ou JSON aninhado via `_json_para_dataframe`); a
tipagem em si nunca é motivo pra subclassear — isso é sempre responsabilidade
do dbt.
"""

import io
import json
import logging
from typing import Any, cast

import pandas as pd

from cliente_minio import (
    download_raw_bytes,
    upload_raw_bytes,
    upload_staging_parquet,
)


def _stringificar(df: pd.DataFrame) -> pd.DataFrame:
    """Cast de toda coluna pra texto, preservando nulo real (nunca a string 'None').

    Também normaliza nome de coluna (strip + lower) — cabeçalho de planilha/CSV
    vem com casing/espaço inconsistente, e isso é estrutural, não tipagem.
    """
    df = df.rename(columns=lambda c: str(c).strip().lower())
    for col in df.columns:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else str(v)).astype("string")
    return df


def registros_para_staging_parquet(
    fonte: str,
    dado: str,
    registros: list[dict[str, Any]],
) -> None:
    """Etapa 02 para fontes API-JSON com `registros` já normalizados pelo cliente.

    Monta o DataFrame a partir dos registros e escreve o parquet (texto) na
    staging. O cliente não deve mais tipar os valores antes de chamar isto —
    só repassar o que a API respondeu.
    """
    if not registros:
        logging.warning(f"[base_file_parser] Sem registros p/ parquet: {fonte}.{dado}")
        return

    df = _stringificar(pd.DataFrame(registros))

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    upload_staging_parquet(fonte, dado, buffer.getvalue())
    logging.info(f"[base_file_parser] staging/{fonte}/{dado}.parquet ({len(df)} linhas)")


class BaseFileParser:
    """Template Method: leva um dado do RAW ao parquet (texto) da STAGING."""

    #: separador default para formato "txt"
    _sep: str = "\t"

    def __init__(self, fonte: str, dado: str, formato: str) -> None:
        self.fonte = fonte
        self.dado = dado
        self.formato = formato  # "csv" | "txt" | "xlsx" | "json"

    # ------------------------- TEMPLATE METHOD (invariante) -------------------
    def gerar_staging_parquet(self) -> None:
        """Executa a Etapa 02: raw -> DataFrame -> texto -> parquet staging."""
        raw = download_raw_bytes(self.fonte, self.dado, ext=self.formato)
        df = self._para_dataframe(raw)
        self._salvar_parquet(df)

    # ------------------------- parse por FORMATO (fechado) --------------------
    def _para_dataframe(self, raw: bytes) -> pd.DataFrame:
        kwargs: dict[str, Any] = {**self._read_kwargs(), "dtype": str}
        if self.formato == "csv":
            return cast(pd.DataFrame, pd.read_csv(io.BytesIO(raw), **kwargs))
        if self.formato == "txt":
            return cast(
                pd.DataFrame, pd.read_csv(io.BytesIO(raw), sep=self._sep, **kwargs)
            )
        if self.formato == "xlsx":
            return cast(pd.DataFrame, pd.read_excel(io.BytesIO(raw), **kwargs))
        if self.formato == "json":
            return self._json_para_dataframe(json.loads(raw.decode("utf-8")))
        raise ValueError(f"[base_file_parser] Formato não suportado: {self.formato}")

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

    def _salvar_parquet(self, df: pd.DataFrame) -> None:
        df = _stringificar(df)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        upload_staging_parquet(self.fonte, self.dado, buffer.getvalue())
        logging.info(
            f"[base_file_parser] staging/{self.fonte}/{self.dado}.parquet "
            f"({len(df)} linhas, {len(df.columns)} colunas)"
        )
