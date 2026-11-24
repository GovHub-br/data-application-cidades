"""raw -> staging: cada CSV do dump vira Parquet todo em texto, com o dado pessoal mascarado."""

import csv
import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from historico_mcmv.erros import ErroDadoPessoal, ErroHistorico
from historico_mcmv.estrutura import DIVIDIR
from historico_mcmv.layout import RAW, STAGING
from historico_mcmv.nomes import normalizar_colunas

log = logging.getLogger(__name__)


# 1. raw -> staging

DELIMITADOR = ";"
ENCODING = "utf-8"  # como a raw foi exportada do dump
CHUNKSIZE = 200_000
LINHAGEM = ["_source_file", "_ingested_at", "_source_hash"]
LIMITE_COLS_SEM_NOME = 10  # acima disso, coluna sem nome e vazia é padding do Excel

# Arquivos da raw que nunca entram no pipeline (ficam só na raw): sem dado do programa.
RAW_FORA = {
    "bb_2015_08_agosto_loginfesta.csv",  # credenciais (login e PIN), nenhum dado do MCMV
}
CAMPOS_MANIFESTO = [
    "raw_key",
    "raw_etag",
    "raw_bytes",
    "staging_key",
    "md5",
    "encoding",
    "delimitador",
    "n_linhas",
    "n_colunas",
    "colunas_mascaradas",
    "status",
    "erro",
    "convertido_em",
]


def chave_staging(raw_key: str) -> str:
    """raw/dados_historicos/<nome>.csv -> <staging><nome>.parquet (espelho um a um)."""
    if not raw_key.startswith(RAW):
        raise ErroHistorico(f"{raw_key} fora de {RAW}")
    return f"{STAGING}{os.path.splitext(raw_key[len(RAW) :])[0]}.parquet"


def md5_arquivo(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 16), b""):
            h.update(bloco)
    return h.hexdigest()


def contar_registros_csv(path: Path, encoding: str) -> int:
    """Registros sem o cabeçalho, pelo csv.reader (um campo aspeado pode quebrar linha)."""
    with open(path, encoding=encoding, errors="replace", newline="") as f:
        total = sum(1 for _ in csv.reader(f, delimiter=DELIMITADOR, quotechar='"'))
    return max(total - 1, 0)


def _ler_csv_args(encoding_errors: str) -> dict:
    return dict(
        sep=DELIMITADOR,
        dtype=str,
        encoding=ENCODING,
        encoding_errors=encoding_errors,
        quotechar='"',
    )


def _colunas_a_manter(src: Path, encoding_errors: str) -> Optional[List[str]]:
    """Colunas sem padding do Excel, ou None quando não há o que limpar (o caso comum)."""
    todas = list(pd.read_csv(src, nrows=0, **_ler_csv_args(encoding_errors)).columns)
    anonimas = [c for c in todas if str(c).startswith("Unnamed:")]
    if len(anonimas) <= LIMITE_COLS_SEM_NOME:
        return None
    com_dado: Set[str] = set()
    for chunk in pd.read_csv(
        src,
        keep_default_na=False,
        na_filter=False,
        chunksize=CHUNKSIZE,
        on_bad_lines="skip",
        usecols=anonimas,
        **_ler_csv_args(encoding_errors),
    ):
        com_dado |= {c for c in chunk.columns if (chunk[c].str.strip() != "").any()}
    manter = [c for c in todas if c not in anonimas or c in com_dado]
    return None if len(manter) == len(todas) else manter


def _gravar_parquet_csv(
    src: Path, dst: Path, meta: dict, encoding_errors: str, ingested_at: str
) -> Tuple[int, int]:
    """Uma passada da conversão: (linhas, colunas). Linha malformada levanta erro."""
    reader = pd.read_csv(
        src,
        keep_default_na=False,
        na_filter=False,
        chunksize=CHUNKSIZE,
        on_bad_lines="error",
        engine="c",
        usecols=_colunas_a_manter(src, encoding_errors),
        **_ler_csv_args(encoding_errors),
    )
    writer, n_linhas, n_colunas = None, 0, 0
    try:
        for chunk in reader:
            if writer is None:
                nomes, mapa = normalizar_colunas([str(c) for c in chunk.columns])
                n_colunas = len(nomes)
                campos = [pa.field(n, pa.string()) for n in nomes + LINHAGEM]
                meta = {**meta, b"column_map": json.dumps(mapa, ensure_ascii=False).encode()}
                schema = pa.schema(campos, metadata=meta)
                writer = pq.ParquetWriter(dst, schema, compression="snappy")
            chunk.columns = nomes
            chunk["_source_file"] = meta[b"source_file"].decode()
            chunk["_ingested_at"] = ingested_at
            chunk["_source_hash"] = meta[b"source_hash"].decode()
            writer.write_table(pa.Table.from_pandas(chunk, schema=schema, preserve_index=False))
            n_linhas += len(chunk)
    finally:
        if writer is not None:
            writer.close()
    return n_linhas, n_colunas


def _parquet_vazio(src: Path, dst: Path, meta: dict) -> int:
    """Arquivo só com cabeçalho (ou nem isso): Parquet vazio com as colunas e a linhagem."""
    with open(src, encoding=ENCODING, errors="replace", newline="") as f:
        header = next(csv.reader(f, delimiter=DELIMITADOR, quotechar='"'), [])
    nomes, _ = normalizar_colunas([str(c) for c in header])
    campos = [pa.field(n, pa.string()) for n in nomes + LINHAGEM]
    pq.write_table(pa.schema(campos, metadata=meta).empty_table(), dst)
    return len(nomes)


def converter_csv(src: Path, dst: Path, source_file: str) -> dict:
    """CSV da raw -> Parquet todo em texto, e confere a contagem de registros.

    UTF-8 estrito; se algum byte não decodifica, refaz com `replace` (só esses bytes viram
    U+FFFD) e isso fica no manifesto como `utf-8+replace`.
    """
    source_hash = md5_arquivo(src)
    ingested_at = datetime.now(timezone.utc).isoformat()
    n_linhas = n_colunas = 0
    for encoding_errors in ("strict", "replace"):
        rotulo = ENCODING if encoding_errors == "strict" else f"{ENCODING}+replace"
        meta = {
            b"source_file": source_file.encode(),
            b"source_hash": source_hash.encode(),
            b"encoding": rotulo.encode(),
            b"delimiter": DELIMITADOR.encode(),
        }
        try:
            n_linhas, n_colunas = _gravar_parquet_csv(src, dst, meta, encoding_errors, ingested_at)
            break
        except UnicodeDecodeError:
            if encoding_errors == "replace":
                raise
            log.warning("%s: não é UTF-8 em algum byte; refazendo com replace", source_file)
        except pd.errors.EmptyDataError:
            break  # tabela sem coluna nenhuma: o espelho continua um a um
    if not dst.exists():
        n_colunas = _parquet_vazio(src, dst, meta)
    n_parquet = pq.ParquetFile(dst).metadata.num_rows
    n_csv = contar_registros_csv(src, ENCODING)
    if not n_parquet == n_linhas == n_csv:
        raise ErroHistorico(f"contagem não fecha: csv={n_csv} lidas={n_linhas} parquet={n_parquet}")
    return {
        "md5": source_hash,
        "encoding": rotulo,
        "delimitador": DELIMITADOR,
        "n_linhas": n_parquet,
        "n_colunas": n_colunas,
    }


# Dado pessoal: só pelo NOME do campo. Nada de lista de arquivos (o repositório é público).
REDACAO = "***"
_PII_CPF = re.compile(r"cpf|(^|_)nis(_|$)|(^|_)pis(_|$)|pasep")
_PII_NOME = re.compile(
    r"nome_(benef|mutu|titular|comprador|conjuge|pessoa)|(^|_)resp(_|$)|responsavel"
)
_PII_CONTATO = re.compile(r"(^|_)tel(_|$)|telefone|celular|(^|_)fone(_|$)|e_?mail")
_PII_ENDERECO = re.compile(r"endereco|logradouro|bairro|complemento|(^|_)cep(_|$)")

# o que prova que o arquivo é de pessoa física (instituição não tem CPF nem raça)
_PESSOA_FISICA = re.compile(r"cpf|(^|_)nis(_|$)|nascimento|cor_raca|(^|_)raca(_|$)")
_CPF_EM_CLARO = re.compile(r"^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$")


def colunas_pii(colunas: List[str]) -> Dict[str, str]:
    """{coluna: categoria} do que é dado pessoal. CPF/NIS é `cpf` (no histórico já vem em
    hash e fica); nome e contato saem sempre; endereço só em arquivo de pessoa física."""
    pessoa_fisica = any(_PESSOA_FISICA.search(c) for c in colunas)
    achadas = {}
    for c in colunas:
        if _PII_CPF.search(c):
            achadas[c] = "cpf"
        elif _PII_NOME.search(c):
            achadas[c] = "nome"
        elif _PII_CONTATO.search(c):
            achadas[c] = "contato"
        elif pessoa_fisica and _PII_ENDERECO.search(c):
            achadas[c] = "endereco"
    return achadas


def _redigir(serie: pd.Series) -> pd.Series:
    cheia = serie.fillna("").str.strip() != ""
    return serie.where(~cheia, REDACAO)


def _conferir_cpf(nome: str, serie: pd.Series) -> None:
    if serie.fillna("").str.strip().str.match(_CPF_EM_CLARO).any():
        raise ErroDadoPessoal(f"{nome}: CPF/NIS em claro; o histórico deveria trazer só hash")


def _mascarar_campos(serie: pd.Series, campos: List[str]) -> Tuple[pd.Series, List[str]]:
    """Coluna única com os campos colados por "|": separa, mascara e junta de novo."""
    nomes, _ = normalizar_colunas(campos)
    alvos = colunas_pii(nomes)
    if not alvos:
        return serie, []
    texto = serie.fillna("")
    # só a linha com o número certo de campos; a torta fica como está (o `dividir` do
    # cleansed recusa o arquivo)
    certas = texto.str.count(r"\|") == len(nomes) - 1
    if not certas.any():
        return serie, []
    partes = texto[certas].str.split("|", n=len(nomes) - 1, expand=True, regex=False)
    for i, nome in enumerate(nomes):
        if alvos.get(nome) == "cpf":
            _conferir_cpf(nome, partes[i])
        elif nome in alvos:
            partes[i] = _redigir(partes[i])
    saida = serie.copy()
    saida[certas] = partes.agg("|".join, axis=1)
    return saida, [n for n in nomes if alvos.get(n) not in (None, "cpf")]


def mascarar_parquet(path: Path, campos_colados: Optional[List[str]] = None) -> List[str]:
    """Mascara o dado pessoal do Parquet da staging no lugar. Devolve o que mascarou.

    `campos_colados`: nomes dos campos de um arquivo de uma coluna só, com tudo colado por
    "|" (a regra `dividir` do mapa de cabeçalhos).
    """
    tabela = pq.read_table(path)
    dados = [c for c in tabela.column_names if not c.startswith("_")]
    df = tabela.to_pandas()
    mascaradas: List[str] = []
    colada = bool(campos_colados) and len(dados) == 1
    # coluna colada: o nome dela é o cabeçalho inteiro grudado ("datacnpj_bbresp_arq..."),
    # então vale o nome de cada campo, não o da coluna
    for coluna, categoria in ({} if colada else colunas_pii(dados)).items():
        if categoria == "cpf":
            _conferir_cpf(coluna, df[coluna])
        else:
            df[coluna] = _redigir(df[coluna])
            mascaradas.append(coluna)
    if colada:
        df[dados[0]], campos = _mascarar_campos(df[dados[0]], campos_colados)
        mascaradas += [f"{dados[0]}:{c}" for c in campos]
    if mascaradas:
        nova = pa.Table.from_pandas(df, schema=tabela.schema, preserve_index=False)
        pq.write_table(nova, path, compression="snappy")
    return mascaradas


def ler_manifesto(texto: Optional[str]) -> Dict[str, dict]:
    """Última linha de cada raw_key."""
    if not texto:
        return {}
    return {linha["raw_key"]: linha for linha in csv.DictReader(io.StringIO(texto))}


def escrever_manifesto(linhas: Iterable[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_MANIFESTO)
        w.writeheader()
        for linha in linhas:
            w.writerow({k: linha.get(k, "") for k in CAMPOS_MANIFESTO})


def ja_convertido(anterior: Optional[dict], etag: str, existe: bool) -> bool:
    return (
        bool(anterior and anterior.get("status") == "ok" and anterior.get("raw_etag") == etag)
        and existe
    )


def campos_colados(mapa: pd.DataFrame) -> Dict[str, List[str]]:
    """arquivo -> nomes dos campos, para os arquivos com regra `dividir`."""
    dividir = mapa[mapa.acao == DIVIDIR]
    return {r.arquivo: r.colunas.split("|") for r in dividir.itertuples()}
