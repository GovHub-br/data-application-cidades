"""Validação dos dados históricos com pandera: o contrato de cada fronteira do pipeline.

- entrada: os mapas de configuração (`cabecalhos.csv`, `familias.csv`), que são editados à
  mão no lake e quebram o tratamento inteiro com uma vírgula fora do lugar;
- controle: o manifesto da staging, os logs do analytics e do cleansed e os descartes;
- saída: a forma de cada arquivo do cleansed (`_linha_origem` inteiro e único, o resto
  texto, com nome de coluna normalizado).

Cada `validar_*` devolve o DataFrame validado ou levanta `ErroHistorico` com os primeiros
casos que falharam (todos de uma vez, não só o primeiro).
"""

import re

import pandas as pd
import pandera.pandas as pa

from historico_mcmv.analytics import COPIA, MODELO_2009, SELECIONADO, VAZIO
from historico_mcmv.erros import ErroHistorico
from historico_mcmv.estrutura import (
    CABECALHO,
    COMPOR,
    COPIA_CABECALHO,
    DIVIDIR,
    DUPLICADA,
    EXCLUIR,
    LINHA_ORIGEM,
    MANTER,
    NOMEAR,
    PROMOVER,
    RODAPE,
    SUBTITULO,
    TITULO,
    TOTAL,
    VAZIA,
    ZERADA,
)
from historico_mcmv.layout import ANALYTICS, CLEANSED, RAW, STAGING, SUBSTITUIDAS

_INTEIRO_OU_VAZIO = r"^\d*$"
_INTEIROS = r"^(\d+(\|\d+)*)?$"  # "9075|9076"
_NOMES = r"^([a-z0-9_]+(\|[a-z0-9_]+)*)?$"  # "cod_apf|codapf"
_NOME_COLUNA = r"^[a-z0-9][a-z0-9_]*$"  # o que normalizar_colunas produz
_COMPETENCIA = r"^\d{4}-(0[1-9]|1[0-2])$"
_MD5 = r"^[0-9a-f]{32}$"
MOTIVOS_DESCARTE = [CABECALHO, TITULO, VAZIA, COPIA_CABECALHO, SUBTITULO, TOTAL, RODAPE]
MOTIVOS_DESCARTE += [ZERADA, DUPLICADA]


def _texto(*checks, **kw) -> pa.Column:
    return pa.Column(str, list(checks), **kw)


def _regex_valida(serie: pd.Series) -> pd.Series:
    def ok(padrao: str) -> bool:
        try:
            re.compile(padrao)
            return True
        except re.error:
            return False

    return serie.map(ok)


def _validar(schema: pa.DataFrameSchema, df: pd.DataFrame, nome: str) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        casos = exc.failure_cases
        resumo = "; ".join(
            f"{r.column or 'tabela'}: {r.check} ({r.failure_case!r}, linha {r.index})"
            for r in casos.head(5).itertuples()
        )
        raise ErroHistorico(f"{nome}: {len(casos)} falha(s) de contrato. {resumo}") from exc


# ---------------------------------------------------------------------------------------
# entrada: mapas de configuração (lidos do lake como texto, vazio = "")
# ---------------------------------------------------------------------------------------

MAPA_CABECALHOS = pa.DataFrameSchema(
    {
        "arquivo": _texto(pa.Check.str_length(min_value=1)),
        "bloco": _texto(),
        "acao": _texto(pa.Check.isin([MANTER, PROMOVER, COMPOR, NOMEAR, DIVIDIR, EXCLUIR])),
        "linha": _texto(pa.Check.str_matches(_INTEIRO_OU_VAZIO)),
        "fim": _texto(pa.Check.str_matches(_INTEIRO_OU_VAZIO)),
        "colunas": _texto(),
        "motivo": _texto(),
        "linhas_zeradas": _texto(pa.Check.isin(["", "descartar"]), required=False),
        "linhas_total": _texto(pa.Check.str_matches(_INTEIROS), required=False),
    },
    strict=True,
    unique=["arquivo", "bloco"],
    checks=pa.Check(
        lambda df: ~df["acao"].isin([NOMEAR, DIVIDIR]) | (df["colunas"] != ""),
        error="nomear e dividir precisam das colunas",
    ),
)

MAPA_FAMILIAS = pa.DataFrameSchema(
    {
        "familia": _texto(pa.Check.str_matches(r"^[a-z0-9_]+$"), unique=True),
        "padrao": _texto(pa.Check(_regex_valida, error="regex inválida")),
        "chave": _texto(pa.Check.str_matches(_NOMES), required=False),
        "fonte": _texto(),
        "descricao": _texto(),
    },
    strict=True,
)


def validar_mapa_cabecalhos(df: pd.DataFrame) -> pd.DataFrame:
    return _validar(MAPA_CABECALHOS, df, "cabecalhos.csv")


def validar_mapa_familias(df: pd.DataFrame) -> pd.DataFrame:
    return _validar(MAPA_FAMILIAS, df, "familias.csv")


# ---------------------------------------------------------------------------------------
# controle de cada camada
# ---------------------------------------------------------------------------------------

MANIFESTO = pa.DataFrameSchema(
    {
        "raw_key": _texto(pa.Check.str_startswith(RAW), unique=True),
        "staging_key": _texto(pa.Check.str_startswith(STAGING)),
        "status": _texto(pa.Check.isin(["ok", "erro"])),
        "md5": _texto(pa.Check.str_matches(f"{_MD5}|^$")),
        "n_linhas": _texto(pa.Check.str_matches(_INTEIRO_OU_VAZIO)),
    },
    checks=pa.Check(
        lambda df: (df["status"] != "ok") | df["md5"].str.match(_MD5),
        error="arquivo convertido (ok) sem md5",
    ),
)


def validar_manifesto(df: pd.DataFrame) -> pd.DataFrame:
    """O manifesto como lido do CSV (tudo texto)."""
    return _validar(MANIFESTO, df, "manifesto da staging")


LOG_ANALYTICS = pa.DataFrameSchema(
    {
        "arquivo": pa.Column(str, unique=True),
        "staging_key": pa.Column(str, pa.Check.str_startswith(STAGING)),
        "status": pa.Column(str, pa.Check.isin([SELECIONADO, COPIA, VAZIO, MODELO_2009])),
        "analytics_key": pa.Column(str, pa.Check.str_startswith(ANALYTICS), nullable=True),
        "copia_de": pa.Column(str, nullable=True),
        "n_linhas": pa.Column("int64", pa.Check.ge(0)),
        "n_colunas": pa.Column("int64", pa.Check.ge(0)),
    },
    checks=[
        pa.Check(
            lambda df: (df["status"] == SELECIONADO) == df["analytics_key"].notna(),
            error="só o selecionado vai para o analytics (e todo selecionado vai)",
        ),
        pa.Check(
            lambda df: (df["status"] == COPIA) == df["copia_de"].notna(),
            error="toda cópia aponta de quem é cópia (e só a cópia)",
        ),
    ],
)


def validar_log_analytics(df: pd.DataFrame) -> pd.DataFrame:
    return _validar(LOG_ANALYTICS, df, "log do analytics")


def _schema_log_cleansed() -> pa.DataFrameSchema:
    # import local: o cleansed valida as próprias saídas por este módulo
    from historico_mcmv.cleansed import EXCLUIDO, SUBSTITUIDA, TRATADO

    def chave_certa(df: pd.DataFrame) -> pd.Series:
        familia = df["familia"].fillna("")
        chave = df["cleansed_key"].fillna("")
        tratado = [c.startswith(f"{CLEANSED}{f}/") for c, f in zip(chave, familia)]
        substituida = chave.str.startswith(SUBSTITUIDAS)
        return (
            ((df["status"] == TRATADO) & pd.Series(tratado, index=df.index))
            | ((df["status"] == SUBSTITUIDA) & substituida)
            | (df["status"] == EXCLUIDO)
        )

    return pa.DataFrameSchema(
        {
            "saida": pa.Column(str, unique=True),
            "status": pa.Column(str, pa.Check.isin([TRATADO, EXCLUIDO, SUBSTITUIDA])),
            "familia": pa.Column(str, nullable=True),
            "cleansed_key": pa.Column(str, nullable=True),
            "competencia": pa.Column(str, pa.Check.str_matches(_COMPETENCIA), nullable=True),
            # inteiro com nulo volta do Parquet como float: só o valor é conferido
            "linhas_entrada": pa.Column(None, pa.Check.ge(0), nullable=True),
            "linhas_reais": pa.Column(None, pa.Check.ge(0), nullable=True),
        },
        checks=[
            pa.Check(chave_certa, error="saída fora da pasta da família (ou da de substituídas)"),
            pa.Check(
                lambda df: (df["status"] == EXCLUIDO) | df["familia"].notna(),
                error="saída tratada sem família",
            ),
        ],
    )


def validar_log_cleansed(df: pd.DataFrame) -> pd.DataFrame:
    return _validar(_schema_log_cleansed(), df, "log do cleansed")


DESCARTES = pa.DataFrameSchema(
    {
        "arquivo": pa.Column(str),
        LINHA_ORIGEM: pa.Column("int64", pa.Check.ge(0)),
        "motivo": pa.Column(str, pa.Check.isin(MOTIVOS_DESCARTE)),
    },
)


def validar_descartes(df: pd.DataFrame) -> pd.DataFrame:
    return _validar(DESCARTES, df, "descartes do cleansed")


# ---------------------------------------------------------------------------------------
# saída: cada arquivo do cleansed
# ---------------------------------------------------------------------------------------

SAIDA_CLEANSED = pa.DataFrameSchema(
    {
        LINHA_ORIGEM: pa.Column("int64", pa.Check.ge(0), unique=True),
        _NOME_COLUNA: pa.Column(str, nullable=True, regex=True),
    },
    strict=True,  # coluna fora do padrão de nome (ou de outro tipo) para o tratamento
)


def validar_saida(df: pd.DataFrame, saida: str) -> pd.DataFrame:
    return _validar(SAIDA_CLEANSED, df, f"saída {saida}")
