"""Template Method da ingestão do data lake do conjuntura (Etapa 02).

Pipeline do conjuntura contínuo:
    Etapa 01 (RAW)     CSV / TXT / XLSX / API-JSON  -> raw/<fonte>/<dado>.<ext>
    Etapa 02 (STAGING) TODOS convertidos p/ parquet -> staging/<fonte>/<dado>.parquet
    Etapa 03 (SILVER)  pg_duckdb read_parquet        -> Postgres (dbt)

Este módulo implementa a Etapa 02 com o padrão Template Method: o esqueleto
(baixar raw -> DataFrame -> tipar -> escrever parquet) é invariante; o que varia
é o *parse por formato* (fechado em csv/txt/xlsx/json) e a *tipagem por dataset*
(hook abstrato `_tipar`).

Para adicionar um dado novo, cria-se uma subclasse definindo `fonte`, `dado`,
`formato` e implementando `_tipar` (e, se preciso, `_read_kwargs`, `_sep` ou
`_json_para_dataframe`).
"""

import io
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, cast

import pandas as pd

from cliente_minio import (
    download_raw_bytes,
    upload_raw_bytes,
    upload_staging_parquet,
)


def num_br(serie: "pd.Series[Any]") -> "pd.Series[Any]":
    """Número em formato pt-BR ('1.234,56' -> 1234.56); '-'/'' -> nulo."""
    nulos: dict[str, Any] = {"": None, "-": None}
    limpa = (
        serie.astype("string")
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace(nulos)
    )
    return pd.to_numeric(limpa, errors="coerce")


def registros_para_staging_parquet(
    fonte: str,
    dado: str,
    registros: list[dict[str, Any]],
    typers: "dict[str, Callable[[pd.Series[Any]], pd.Series[Any]]] | None" = None,
) -> None:
    """Etapa 02 para fontes API-JSON, persistida integralmente como texto.

    A staging e a bronze não fazem tipagem analítica. ``typers`` permanece na
    assinatura para compatibilidade com DAGs antigas, mas é ignorado.
    """
    if not registros:
        logging.warning(f"[ingestor_lake] Sem registros p/ parquet: {fonte}.{dado}")
        return

    df = pd.DataFrame(registros)
    if typers:
        logging.warning("[ingestor_lake] typers ignorados: staging e bronze são textuais")
    df = _todas_colunas_texto(df)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    upload_staging_parquet(fonte, dado, buffer.getvalue())
    logging.info(f"[ingestor_lake] staging/{fonte}/{dado}.parquet ({len(df)} linhas)")


class IngestorLake(ABC):
    """Base Template Method: leva um dado do RAW ao parquet TIPADO da STAGING."""

    #: identificação do dado no lake (subclasse obrigatoriamente define)
    fonte: str
    dado: str
    formato: str  # "csv" | "txt" | "xlsx" | "json"

    #: separador default para formato "txt"
    _sep: str = "\t"

    # ------------------------- TEMPLATE METHOD (invariante) -------------------
    def gerar_staging_parquet(self) -> None:
        """Executa a Etapa 02: raw -> DataFrame -> tipagem -> parquet staging."""
        raw = download_raw_bytes(self.fonte, self.dado, ext=self.formato)
        df = self._para_dataframe(raw)
        df = self._tipar(df)
        self._salvar_parquet(df)

    # ------------------------- parse por FORMATO (fechado) --------------------
    def _para_dataframe(self, raw: bytes) -> pd.DataFrame:
        if self.formato == "csv":
            return cast(pd.DataFrame, pd.read_csv(io.BytesIO(raw), **self._read_kwargs()))
        if self.formato == "txt":
            return cast(
                pd.DataFrame,
                pd.read_csv(io.BytesIO(raw), sep=self._sep, **self._read_kwargs()),
            )
        if self.formato == "xlsx":
            return cast(
                pd.DataFrame, pd.read_excel(io.BytesIO(raw), **self._read_kwargs())
            )
        if self.formato == "json":
            return self._json_para_dataframe(json.loads(raw.decode("utf-8")))
        raise ValueError(f"[ingestor_lake] Formato não suportado: {self.formato}")

    # ------------------------- hooks com default -----------------------------
    def _read_kwargs(self) -> dict[str, Any]:
        """kwargs extras p/ pandas.read_csv/read_excel (header, sheet_name...)."""
        return {}

    def _json_para_dataframe(self, obj: Any) -> pd.DataFrame:
        """Default: json já 'flat' -> DataFrame. Override p/ json aninhado (IBGE)."""
        return pd.DataFrame(obj)

    @staticmethod
    def _num_br(serie: "pd.Series[Any]") -> "pd.Series[Any]":
        """Número em formato pt-BR ('1.234,56' -> 1234.56); '-'/'' -> nulo."""
        return num_br(serie)

    # ------------------------- hook OBRIGATÓRIO ------------------------------
    @abstractmethod
    def _tipar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica os tipos finais do dataset — o parquet precisa sair tipado."""

    # ------------------------- infra (invariante) ----------------------------
    def subir_raw(
        self, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Conveniência da Etapa 01: sobe o arquivo cru recebido para a raw."""
        upload_raw_bytes(
            self.fonte, self.dado, data, ext=self.formato, content_type=content_type
        )

    def _salvar_parquet(self, df: pd.DataFrame) -> None:
        df = _todas_colunas_texto(df)
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        upload_staging_parquet(self.fonte, self.dado, buffer.getvalue())
        logging.info(
            f"[ingestor_lake] staging/{self.fonte}/{self.dado}.parquet "
            f"({len(df)} linhas, {len(df.columns)} colunas)"
        )


def _todas_colunas_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Evita inferência de tipos no parquet; JSON aninhado vira texto JSON."""

    def serializar(valor: Any) -> str | None:
        if valor is None:
            return None
        if isinstance(valor, (dict, list)):
            return json.dumps(valor, ensure_ascii=False, default=str)
        if pd.isna(valor):
            return None
        if hasattr(valor, "isoformat"):
            return valor.isoformat()
        return str(valor)

    return df.apply(lambda coluna: coluna.map(serializar).astype("string"))


class IngestorBalancoEmpresas(IngestorLake):
    """Balanços das construtoras — lançamentos e vendas (dado manual, XLSX).

    Página 02 do boletim. Empresas: MRV, Cury, Tenda, Direcional, Pacaembu,
    Plano & Plano. Serve de template p/ os demais dados manuais: só muda
    `formato`, os nomes de coluna e os casts em `_tipar`.

    Ajuste `_read_kwargs` (sheet_name/header) e os nomes de coluna ao layout
    real da planilha enviada pelo CEAG.
    """

    fonte = "empresas"
    dado = "balanco_lancamentos_vendas"
    formato = "xlsx"

    _COLUNAS_NUMERICAS = [
        "lancamentos",
        "lancamentos_tri_anterior",
        "lancamentos_mesmo_tri_ano_anterior",
        "lancamento_acumulado_mesmo_periodo_ano_anterior",
        "vendas",
        "vendas_tri_anterior",
        "vendas_mesmo_tri_ano_anterior",
        "vendas_acumulado_mesmo_periodo_ano_anterior",
    ]

    def _tipar(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns=lambda c: str(c).strip().lower())

        df["periodo"] = df["periodo"].astype("string").str.strip().str.upper()
        df["empresa"] = df["empresa"].astype("string").str.strip()
        for col in ["trimestre", "ano"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # XLSX já entrega números nativos; to_numeric é só a rede de segurança.
        for col in self._COLUNAS_NUMERICAS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
