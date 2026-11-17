"""Dados históricos do MCMV: raw/dados_historicos/ -> staging -> analytics -> cleansed.

A raw é o dump congelado do banco antigo (754 CSVs `;`), subido à mão e completo no MinIO.
Ela é o Asset `dados_historicos_raw`: quem sobe cria um evento do Asset na UI (ou dispara
a DAG direto). Cada camada desta DAG é um Asset também, para quem vier depois (a bronze)
agendar por ele.

Camadas, todas sob `staging/historico_mcmv/` (`convertido/`, `analytics/`, `cleansed/`):
- **staging**: cada CSV vira Parquet, um a um, tudo em texto, sem tocar no conteúdo, menos
  o dado pessoal, que é mascarado aqui (a raw fica como foi subida);
- **analytics**: só os arquivos de conteúdo próprio (fora: vazios, o modelo de 2009 que o
  dump gravou no lugar de 273 tabelas e as cópias exatas), copiados como estão;
- **cleansed**: tratamento estrutural, uma pasta por família. Desfaz a duplicação 2x da
  exportação, arruma o cabeçalho, separa o que não é dado (com registro de cada descarte),
  confere que nada se perdeu e data cada arquivo (competência e data de corte). Depois
  padroniza a escrita dos valores (ainda texto: ponto decimal, sem `.0`, datas AAAA-MM-DD,
  sem espaço nas pontas, nulos escritos vazios, CNPJ com 14 dígitos), conferindo cada
  célula alterada; tira linha idêntica repetida nas famílias com chave; e deixa uma versão
  por conteúdo (as outras vão para `_versoes_substituidas/`, fora da pasta da família);
- **colunas**: evidência para o grafo de nomes de coluna (como cada dado muda de nome ao
  longo do tempo) e a proposta de nome canônico.

Os mapas que dizem como tratar cada arquivo NÃO ficam no repositório: ficam no MinIO, em
`staging/historico_mcmv/config/` (`cabecalhos.csv`: cabeçalho, blocos e colunas coladas
por "|"; `familias.csv`: a família de cada saída).

Conexão: `minio_lake`, do tipo `aws`, com o `endpoint_url` do MinIO no extra, por exemplo
`AIRFLOW_CONN_MINIO_LAKE='{"conn_type": "aws", "login": "<access key>", "password":
"<secret>", "extra": {"endpoint_url": "http://10.0.0.56:9000"}}'`.

Params da staging (disparo manual):
- `forcar`: reconverte mesmo o que já está convertido (por padrão só o que mudou na raw,
  pelo etag);
- `filtro`: regex sobre o nome do arquivo; só os que casam passam pela staging (o
  analytics e o cleansed sempre refazem tudo).
"""

import csv
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import Asset, dag, get_current_context, task

log = logging.getLogger(__name__)

BUCKET = "data-lake-mcid"
CONN_ID = "minio_lake"
RAW = "raw/dados_historicos/"
# Tudo sob staging/historico_mcmv/: staging/dados_historicos/ é da conversão antiga do dump.
BASE = "staging/historico_mcmv/"
STAGING = BASE + "convertido/"
ANALYTICS = BASE + "analytics/"
CLEANSED = BASE + "cleansed/"
CONFIG = BASE + "config/"
MAPA_CABECALHOS = CONFIG + "cabecalhos.csv"
MAPA_FAMILIAS = CONFIG + "familias.csv"

ASSET_RAW = Asset(name="dados_historicos_raw", uri=f"s3://{BUCKET}/{RAW}")
ASSET_STAGING = Asset(name="dados_historicos_staging", uri=f"s3://{BUCKET}/{STAGING}")
ASSET_ANALYTICS = Asset(name="dados_historicos_analytics", uri=f"s3://{BUCKET}/{ANALYTICS}")
ASSET_CLEANSED = Asset(name="dados_historicos_cleansed", uri=f"s3://{BUCKET}/{CLEANSED}")

MB = 1024 * 1024


class ErroHistorico(ValueError):
    """O dado não pode seguir sem perder, inventar ou expor informação."""


class ErroDadoPessoal(ErroHistorico):
    """Dado pessoal em claro onde o histórico deveria trazer só hash: para tudo."""


# Lake: o bucket pelo S3Hook


class Lake:
    """Acesso ao bucket pelo S3Hook do provider amazon. As etapas recebem um Lake; os
    testes passam um em memória."""

    def __init__(self, hook=None):
        if hook is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            # o MinIO do lake falha com 10 conexões por arquivo: 2, em partes de 8 MB
            hook = S3Hook(
                aws_conn_id=CONN_ID,
                transfer_config_args={
                    "max_concurrency": 2,
                    "multipart_chunksize": 8 * MB,
                    "multipart_threshold": 8 * MB,
                },
            )
        self.hook = hook

    def listar(self, prefixo: str) -> List[Tuple[str, int, str]]:
        """(key, tamanho, etag) de cada objeto sob o prefixo."""
        return [
            (o["Key"], int(o["Size"]), str(o.get("ETag", "")).strip('"'))
            for o in self.hook.iter_file_metadata(prefix=prefixo, bucket_name=BUCKET)
        ]

    def baixar(self, key: str, destino: Path) -> Path:
        destino.parent.mkdir(parents=True, exist_ok=True)
        self.hook.get_conn().download_file(
            BUCKET, key, str(destino), Config=self.hook.transfer_config
        )
        return destino

    def subir(self, local: Path, key: str) -> None:
        self.hook.load_file(str(local), key, bucket_name=BUCKET, replace=True)

    def copiar(self, origem: str, destino: str) -> None:
        self.hook.copy_object(origem, destino, source_bucket_name=BUCKET, dest_bucket_name=BUCKET)

    def apagar(self, keys: Iterable[str]) -> None:
        keys = list(keys)
        for i in range(0, len(keys), 1000):
            self.hook.delete_objects(BUCKET, keys[i : i + 1000])

    def ler_texto(self, key: str) -> Optional[str]:
        if not self.hook.check_for_key(key, bucket_name=BUCKET):
            return None
        return self.hook.read_key(key, bucket_name=BUCKET)


def ler_csv(lake: Lake, key: str) -> pd.DataFrame:
    texto = lake.ler_texto(key)
    if texto is None:
        raise ErroHistorico(f"{key} não existe no lake")
    return pd.read_csv(io.StringIO(texto), dtype=str, keep_default_na=False)


# Nomes de coluna (a mesma regra do resto do lake: scripts/lake_utils.normalizar_colunas)

_MARCAS_MOJIBAKE = ("Ã", "Â", "â€")


def _corrigir_mojibake(texto: str) -> str:
    if not texto or not any(m in texto for m in _MARCAS_MOJIBAKE):
        return texto
    try:
        return texto.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return texto


def norm_header(texto: str) -> str:
    """Nome de coluna em snake_case ASCII."""
    texto = _corrigir_mojibake(texto)
    s = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip().strip('"').strip("﻿").strip()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def normalizar_colunas(header: List[str]) -> Tuple[List[str], dict]:
    """Normaliza e desambigua (`_2`, `_3`...); cabeçalho vazio vira `coluna_<i>`."""
    finais: List[str] = []
    usados: Set[str] = set()
    mapa: dict = {}
    for i, original in enumerate(header, 1):
        base = norm_header(original) or f"coluna_{i}"
        nome, n = base, 2
        while nome in usados:
            nome, n = f"{base}_{n}", n + 1
        usados.add(nome)
        finais.append(nome)
        mapa[original] = nome
    return finais, mapa


# ---------------------------------------------------------------------------------------
# 1. raw -> staging
# ---------------------------------------------------------------------------------------

DELIMITADOR = ";"
ENCODING = "utf-8"  # como a raw foi exportada do dump
CHUNKSIZE = 200_000
LINHAGEM = ["_source_file", "_ingested_at", "_source_hash"]
LIMITE_COLS_SEM_NOME = 10  # acima disso, coluna sem nome e vazia é padding do Excel
MANIFESTO = STAGING + "_manifesto.csv"
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


def _converter_um(lake: Lake, key: str, etag: str, size: int, colados, tmp: Path) -> dict:
    src, dst = tmp / "entrada.csv", tmp / "saida.parquet"
    destino = chave_staging(key)
    linha = {"raw_key": key, "raw_etag": etag, "raw_bytes": size, "staging_key": destino}
    try:
        for p in (src, dst):
            p.unlink(missing_ok=True)
        lake.baixar(key, src)
        info = converter_csv(src, dst, source_file=key)
        mascaradas = mascarar_parquet(dst, colados.get(Path(destino).stem))
        lake.subir(dst, destino)
        linha.update(info, colunas_mascaradas="|".join(mascaradas), status="ok")
    except ErroDadoPessoal:
        raise  # dado pessoal em claro: não segue para lugar nenhum
    except Exception as exc:  # um arquivo ruim não para os outros; o resumo falha no fim
        linha.update(status="erro", erro=f"{type(exc).__name__}: {exc}"[:500])
    linha["convertido_em"] = datetime.now(timezone.utc).isoformat()
    return linha


def campos_colados(mapa: pd.DataFrame) -> Dict[str, List[str]]:
    """arquivo -> nomes dos campos, para os arquivos com regra `dividir`."""
    dividir = mapa[mapa.acao == DIVIDIR]
    return {r.arquivo: r.colunas.split("|") for r in dividir.itertuples()}


def etapa_staging(lake: Lake, tmp: Path, forcar: bool = False, filtro: str = "") -> Dict[str, int]:
    """Converte a raw para a staging. `filtro` (regex sobre o nome) limita aos arquivos que
    casam; com `forcar`, reconverte esses mesmo sem mudança na raw."""
    anteriores = {
        k: v
        for k, v in ler_manifesto(lake.ler_texto(MANIFESTO)).items()
        if Path(k).name not in RAW_FORA
    }
    existentes = {k for k, _, _ in lake.listar(STAGING)}
    colados = campos_colados(ler_csv(lake, MAPA_CABECALHOS))
    padrao = re.compile(filtro) if filtro else None
    objetos = [
        o
        for o in lake.listar(RAW)
        if o[0].lower().endswith(".csv")
        and Path(o[0]).name not in RAW_FORA
        and (not padrao or padrao.search(Path(o[0]).name))
    ]
    resumo = {"ok": 0, "pulado": 0, "erro": 0}
    for i, (key, size, etag) in enumerate(objetos, 1):
        if not forcar and ja_convertido(
            anteriores.get(key), etag, chave_staging(key) in existentes
        ):
            resumo["pulado"] += 1
            continue
        linha = _converter_um(lake, key, etag, size, colados, tmp)
        anteriores[key] = linha
        resumo[linha["status"]] += 1
        if linha["status"] == "erro":
            log.error("%s: %s", key, linha["erro"])
        if i % 50 == 0:
            log.info("[%d/%d] staging", i, len(objetos))
    escrever_manifesto(anteriores.values(), tmp / "_manifesto.csv")
    lake.subir(tmp / "_manifesto.csv", MANIFESTO)
    log.info("staging: %s", resumo)
    if resumo["erro"]:
        raise ErroHistorico(f"{resumo['erro']} arquivo(s) com erro na staging (ver o manifesto)")
    return resumo


def conferir_raw_contra_manifesto(lake: Lake) -> Dict[str, int]:
    """A raw é subida à mão: falha se sumiu algum arquivo que já foi convertido (upload
    pela metade). Arquivo novo ou alterado só aparece no resumo."""
    raw = {
        k: e
        for k, _, e in lake.listar(RAW)
        if k.lower().endswith(".csv") and Path(k).name not in RAW_FORA
    }
    if not raw:
        raise ErroHistorico(f"{RAW} está vazia")
    anteriores = ler_manifesto(lake.ler_texto(MANIFESTO))
    faltando = sorted(k for k in anteriores if k not in raw)
    if faltando:
        raise ErroHistorico(
            f"{len(faltando)} arquivo(s) do manifesto sumiram da raw: {faltando[:5]}"
        )
    alterados = sum(1 for k, e in raw.items() if k in anteriores and anteriores[k]["raw_etag"] != e)
    resumo = {
        "arquivos": len(raw),
        "novos": len(set(raw) - set(anteriores)),
        "alterados": alterados,
    }
    log.info("raw: %s", resumo)
    return resumo


# ---------------------------------------------------------------------------------------
# 2. staging -> analytics
# ---------------------------------------------------------------------------------------

LOG_ANALYTICS = ANALYTICS + "_analytics_log.parquet"
SELECIONADO, COPIA, VAZIO, MODELO_2009 = "selecionado", "copia", "vazio", "modelo_2009_sem_dado"
# cabeçalho do modelo de abr-jun/2009 que o dump gravou no lugar de 273 tabelas
COLUNAS_MODELO_2009 = frozenset(
    {"unnamed_0", "20090401_000000", "20090501_000000", "20090601_000000"}
)
_MESES = ("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")
_P_ANO_MES = re.compile(r"(?<!\d)((?:19|20)\d{2})_?(0[1-9]|1[0-2])(?!\d)")
_P_MES_ANO = re.compile(r"(?<![a-z])(" + "|".join(_MESES) + r")[a-zç]*_?((?:19|20)?\d{2})(?!\d)")
_P_ANO_MES_DIA = re.compile(r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])(?!\d)")
_P_DIA_MES_ANO = re.compile(
    r"(?<!\d)(?:0[1-9]|[12]\d|3[01])_?(0[1-9]|1[0-2])_?((?:19|20)?\d{2})(?!\d)"
)
_P_ANO = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")

SCHEMA_ANALYTICS = pa.schema(
    [
        ("arquivo", pa.string()),
        ("staging_key", pa.string()),
        ("staging_etag", pa.string()),
        ("analytics_key", pa.string()),
        ("status", pa.string()),
        ("copia_de", pa.string()),
        ("periodo", pa.string()),
        ("n_linhas", pa.int64()),
        ("n_colunas", pa.int64()),
        ("celulas_preenchidas", pa.int64()),
        ("layout", pa.string()),
        ("colunas", pa.list_(pa.string())),
        ("hash_conteudo", pa.string()),
        ("gerado_em", pa.string()),
    ]
)


def periodo(nome: str) -> str:
    """Ano-mês do nome ("AAAAMM"; só o ano, "AAAA00"; nada, ""). Serve para escolher, entre
    cópias, a mais antiga."""
    m = _P_ANO_MES.search(nome)
    if m:
        return m.group(1) + m.group(2)
    m = _P_MES_ANO.search(nome)
    if m:
        ano = m.group(2) if len(m.group(2)) == 4 else "20" + m.group(2)
        return f"{ano}{_MESES.index(m.group(1)) + 1:02d}"
    m = _P_ANO_MES_DIA.search(nome)
    if m:
        return m.group(1) + m.group(2)
    m = _P_DIA_MES_ANO.search(nome)
    if m:
        ano = m.group(2) if len(m.group(2)) == 4 else "20" + m.group(2)
        return ano + m.group(1)
    m = _P_ANO.search(nome)
    return m.group(1) + "00" if m else ""


def _q(coluna: str) -> str:
    return '"' + coluna.replace('"', '""') + '"'


METRICAS_ANALYTICS = (
    "n_linhas",
    "n_colunas",
    "celulas_preenchidas",
    "layout",
    "colunas",
    "hash_conteudo",
)


def metricas_parquet(path: Path, con=None) -> dict:
    """Linhas, colunas de dado (sem as `_*`), células preenchidas e hash do conteúdo (md5,
    independente da ordem das linhas e dos nomes das colunas)."""
    import duckdb

    con = con or duckdb.connect()
    fonte = f"read_parquet('{path}')"
    colunas = [c for c in pq.read_schema(path).names if not c.startswith("_")]
    n_linhas = con.execute(f"select count(*) from {fonte}").fetchone()[0]
    r = {
        "n_linhas": n_linhas,
        "n_colunas": len(colunas),
        "colunas": colunas,
        "layout": hashlib.md5("|".join(sorted(colunas)).encode()).hexdigest()[:8],
        "celulas_preenchidas": 0,
        "hash_conteudo": None,
    }
    if not colunas or not n_linhas:
        return r
    cheias = " + ".join(f"count(nullif(trim({_q(c)}), ''))" for c in colunas)
    linha = ", ".join(f"coalesce({_q(c)}, chr(0))" for c in colunas)
    r["celulas_preenchidas"], r["hash_conteudo"] = con.execute(
        f"""select (select {cheias} from {fonte}),
                   (select md5(string_agg(h, ',' order by h))
                    from (select md5(concat_ws(chr(31), {linha})) as h from {fonte}))"""
    ).fetchone()
    return r


def classificar(registros: Iterable[dict]) -> List[dict]:
    """`status` e `copia_de`. Entre conteúdos idênticos fica o de menor (período, nome)."""
    regs = [dict(r) for r in registros]
    representante: Dict[str, str] = {}
    for r in sorted(regs, key=lambda r: (r["periodo"] or "999999", r["arquivo"])):
        r["copia_de"] = None
        if not r["n_colunas"] or not r["n_linhas"] or not r["celulas_preenchidas"]:
            r["status"] = VAZIO
        elif set(r["colunas"]) == COLUNAS_MODELO_2009:
            r["status"] = MODELO_2009
        elif r["hash_conteudo"] in representante:
            r["status"], r["copia_de"] = COPIA, representante[r["hash_conteudo"]]
        else:
            r["status"] = SELECIONADO
            representante[r["hash_conteudo"]] = r["arquivo"]
    return sorted(regs, key=lambda r: r["arquivo"])


def etapa_analytics(lake: Lake, tmp: Path) -> Dict[str, int]:
    import duckdb

    con = duckdb.connect()
    gerado_em = datetime.now(timezone.utc).isoformat()
    registros = []
    # a raw é congelada: arquivo da staging com o mesmo etag reaproveita as métricas do log
    anteriores = {}
    if any(k == LOG_ANALYTICS for k, _, _ in lake.listar(ANALYTICS)):
        log_ant = pq.read_table(lake.baixar(LOG_ANALYTICS, tmp / "analytics_log_anterior.parquet"))
        anteriores = {r["staging_key"]: r for r in log_ant.to_pylist() if r.get("staging_etag")}
    objetos = [o for o in lake.listar(STAGING) if o[0].endswith(".parquet")]
    reaproveitados = 0
    for i, (key, _, etag) in enumerate(objetos, 1):
        nome = Path(key).stem
        anterior = anteriores.get(key)
        if anterior and anterior["staging_etag"] == etag:
            metricas = {c: anterior[c] for c in METRICAS_ANALYTICS}
            reaproveitados += 1
        else:
            local = lake.baixar(key, tmp / f"{nome}.parquet")
            metricas = metricas_parquet(local, con)
            local.unlink()
        registros.append(
            {
                "arquivo": nome,
                "staging_key": key,
                "staging_etag": etag,
                "periodo": periodo(nome),
                "gerado_em": gerado_em,
                **metricas,
            }
        )
        if i % 100 == 0:
            log.info("[%d/%d] métricas", i, len(objetos))
    registros = classificar(registros)
    for r in registros:
        r["analytics_key"] = (
            f"{ANALYTICS}{r['arquivo']}.parquet" if r["status"] == SELECIONADO else None
        )
    selecionados = {r["analytics_key"] for r in registros if r["analytics_key"]}
    for r in registros:
        if r["analytics_key"]:
            lake.copiar(r["staging_key"], r["analytics_key"])
    sobra = [
        k for k, _, _ in lake.listar(ANALYTICS)
        if k.endswith(".parquet") and k != LOG_ANALYTICS and k not in selecionados
    ]  # fmt: skip
    lake.apagar(sobra)
    linhas = [{c: r.get(c) for c in SCHEMA_ANALYTICS.names} for r in registros]
    pq.write_table(pa.Table.from_pylist(linhas, schema=SCHEMA_ANALYTICS), tmp / "log.parquet")
    lake.subir(tmp / "log.parquet", LOG_ANALYTICS)
    resumo: Dict[str, int] = {}
    for r in registros:
        resumo[r["status"]] = resumo.get(r["status"], 0) + 1
    log.info(
        "analytics: %s (%d removido(s) da seleção anterior; %d métricas reaproveitadas)",
        resumo,
        len(sobra),
        reaproveitados,
    )
    return resumo


# ---------------------------------------------------------------------------------------
# 3. analytics -> cleansed: tratamento estrutural de um arquivo
# ---------------------------------------------------------------------------------------
# A ordem importa: (1) desfazer a duplicação 2x antes de mexer no cabeçalho (senão a 2ª
# cópia da linha de cabeçalho vira dado), (2) cabeçalho, (3) separar o que não é dado,
# (4) tirar coluna vazia sem nome, (5) conferir que nada se perdeu. Os valores não mudam:
# continuam texto, sem trim e sem tipagem (isso é da silver).

MANTER, PROMOVER, COMPOR, NOMEAR, DIVIDIR, EXCLUIR = (
    "manter",
    "promover",
    "compor",
    "nomear",
    "dividir",
    "excluir",
)
CABECALHO, TITULO, VAZIA, COPIA_CABECALHO = "cabecalho", "titulo", "vazia", "copia_cabecalho"
SUBTITULO, TOTAL, RODAPE = "subtitulo", "total", "rodape"
ZERADA = "zerada"  # linha-modelo do Excel: só zeros (e no máximo um rótulo), com regra no mapa
DUPLICADA = "duplicada"  # linha idêntica repetida em família com chave
LINHA_ORIGEM = "_linha_origem"
_NUMERO = re.compile(r"^[\d\s.,%:/()+-]+$")
# total: começa ("TOTAL FASE I"), termina ("SP Total") ou "total fase" no meio; "BRASIL"
# sozinho é o total nacional. "TOTAL" no meio de um nome ("BELLA CITTA TOTAL VILLE") é dado.
_TOTAL = re.compile(r"^total\b|\btotal$|\btotal fase\b|^brasil$")
_DATA_SOLTA = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^\d{2}/\d{2}/\d{4}$")
_SEM_NOME = re.compile(r"^(unnamed|coluna)_\d+$")


@dataclass
class Regra:
    """Cabeçalho de um bloco. `linha` e `fim` contam no arquivo já sem a duplicação 2x.
    Com tabelas empilhadas, uma regra por bloco, cada uma com `bloco` e `fim` (exclusivo;
    o último bloco vai até o fim)."""

    acao: str = MANTER
    linha: int = 0
    motivo: str = ""
    bloco: str = ""
    fim: Optional[int] = None
    colunas: List[str] = field(default_factory=list)  # nomes para nomear/dividir
    zeradas: bool = False  # descarta as linhas-modelo zeradas (regra explícita no mapa)
    totais: List[int] = field(default_factory=list)  # _linha_origem de total sem rótulo (mapa)


@dataclass
class Resultado:
    dados: pd.DataFrame  # colunas finais + _linha_origem
    descartes: pd.DataFrame  # _linha_origem, motivo, valores
    colunas_removidas: List[str] = field(default_factory=list)
    linhas_entrada: int = 0
    colunas_vazias: List[str] = field(default_factory=list)  # com nome, mantidas

    @property
    def por_motivo(self) -> Dict[str, int]:
        return self.descartes.motivo.value_counts().to_dict() if len(self.descartes) else {}


def desfazer_duplicacao(df: pd.DataFrame) -> pd.DataFrame:
    """De cada grupo de n linhas idênticas, mantém as n/2 primeiras ocorrências.

    A exportação do dump gravou toda linha 2x, em páginas embaralhadas. Repetição legítima
    (4 cópias = 2 reais) continua; grupo ímpar quer dizer que a premissa não vale.
    """
    df = df.reset_index(drop=True)
    if df.empty:
        return df.assign(**{LINHA_ORIGEM: pd.Series(dtype="int64")})
    chave = pd.util.hash_pandas_object(df, index=False)
    n = chave.map(chave.value_counts())
    impares = int((n % 2 == 1).sum())
    if impares:
        raise ErroHistorico(f"{impares} linha(s) em grupo de contagem ímpar: a duplicação não é 2x")
    manter = chave.groupby(chave).cumcount() < n // 2
    return df[manter].assign(**{LINHA_ORIGEM: df.index[manter]}).reset_index(drop=True)


def _texto(v) -> str:
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _nomes_compostos(grupo: List[str], coluna: List[str]) -> List[str]:
    """Nome = grupo + coluna, para cabeçalho em dois níveis. O grupo (célula mesclada) vale
    para a direita só onde há sub-cabeçalho; rótulo de grupo centralizado passa ao próximo
    grupo quando um sub-cabeçalho se repete."""
    atual, nomes, vistos = "", [], set()
    for i, (g, c) in enumerate(zip(grupo, coluna)):
        if g.strip() and g.strip() != atual:
            atual, vistos = g.strip(), set()
        sub = c.strip()
        if sub and sub in vistos:
            proximo = next((x.strip() for x in grupo[i + 1 :] if x.strip()), "")
            if proximo and proximo != atual:
                atual, vistos = proximo, set()
        if sub:
            vistos.add(sub)
            partes = [p for p in (atual, sub) if p]
        else:
            partes = [g.strip()] if g.strip() else []
        nomes.append(" ".join(dict.fromkeys(partes)))
    return nomes


def _vazio_descartes() -> pd.DataFrame:
    return pd.DataFrame(
        {LINHA_ORIGEM: pd.Series(dtype="int64"), "motivo": pd.Series(dtype=str), "valores": []}
    )


def _descartes(df: pd.DataFrame, cols: List[str], motivo) -> pd.DataFrame:
    if df.empty:
        return _vazio_descartes()
    return pd.DataFrame(
        {
            LINHA_ORIGEM: df[LINHA_ORIGEM].to_numpy(),
            "motivo": motivo if isinstance(motivo, str) else motivo.to_numpy(),
            "valores": [[_texto(v) for v in linha] for linha in df[cols].itertuples(index=False)],
        }
    )


def aplicar_cabecalho(
    df: pd.DataFrame, regra: Regra
) -> Tuple[pd.DataFrame, pd.DataFrame, List[List[str]]]:
    """(dados, linhas consumidas, linhas de referência do cabeçalho)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    if regra.acao == MANTER:
        return df, _vazio_descartes(), [cols]
    if regra.acao == NOMEAR:
        if len(regra.colunas) != len(cols):
            raise ErroHistorico(
                f"nomear: o mapa dá {len(regra.colunas)} nome(s) para {len(cols)} coluna(s)"
            )
        finais, _ = normalizar_colunas(regra.colunas)
        dados = df.copy()
        dados.columns = finais + [LINHA_ORIGEM]
        return dados, _vazio_descartes(), [finais]
    n_cab = {PROMOVER: 1, COMPOR: 2}.get(regra.acao)
    if n_cab is None:
        raise ErroHistorico(f"ação de cabeçalho desconhecida: {regra.acao}")
    fim = regra.linha + n_cab
    if len(df) < fim:
        raise ErroHistorico(
            f"arquivo tem {len(df)} linha(s); cabeçalho pedido até a linha {fim - 1}"
        )
    linhas_cab = [[_texto(v) for v in df.iloc[i][cols]] for i in range(regra.linha, fim)]
    nomes = linhas_cab[0] if regra.acao == PROMOVER else _nomes_compostos(*linhas_cab)
    # "%" some na normalização ("% Meta R$" e "Meta R$" virariam o mesmo nome): vira "pct"
    finais, _ = normalizar_colunas([n.replace("%", " pct ") for n in nomes])
    consumidas = pd.concat(
        [
            _descartes(df.iloc[: regra.linha], cols, TITULO),
            _descartes(df.iloc[regra.linha : fim], cols, CABECALHO),
        ]
    )
    dados = df.iloc[fim:].copy()
    dados.columns = finais + [LINHA_ORIGEM]
    return dados.reset_index(drop=True), consumidas, linhas_cab


def _normalizado(serie: pd.Series) -> pd.Series:
    unicos = {v: norm_header(v) for v in serie.dropna().unique()}
    return serie.map(unicos).fillna("")


def _copias_de_cabecalho(df: pd.DataFrame, cols: List[str], refs: List[List[str]]) -> pd.Series:
    """Linhas cujas células preenchidas repetem uma linha de cabeçalho na mesma posição
    (pelo menos 2 células, 80% delas iguais, comparando normalizado)."""
    achou = pd.Series(False, index=df.index)
    for ref in refs:
        ref_norm = [norm_header(r) for r in ref]
        posicoes = [i for i, r in enumerate(ref_norm) if r]
        if len(posicoes) < 2:
            continue
        # pré-filtro barato nas 3 primeiras posições; só as candidatas são normalizadas
        cand = pd.Series(False, index=df.index)
        for i in posicoes[:3]:
            alvo = {ref[i].strip().lower(), ref_norm[i]}
            cand |= df[cols[i]].fillna("").str.strip().str.lower().isin(alvo)
        if not cand.any():
            continue
        sub = df[cand]
        iguais = pd.DataFrame(
            {i: _normalizado(sub[cols[i]]) == ref_norm[i] for i in posicoes}, index=sub.index
        ).sum(axis=1)
        cheias = (sub[[cols[i] for i in posicoes]].apply(lambda s: s.str.strip() != "")).sum(axis=1)
        achou[sub.index[(iguais >= 2) & (iguais >= 0.8 * cheias)]] = True
    return achou


def _marcar_rodape(motivo: pd.Series, n_cheias: pd.Series) -> None:
    """Linhas de uma célula só no fim do bloco (total geral sem rótulo)."""
    for i in reversed(motivo.index):
        if motivo[i] == "" and n_cheias[i] == 1:
            motivo[i] = RODAPE
        elif motivo[i] == "":
            break


_ZERO = re.compile(r"^-?0+([.,]0+)?$")


def _zeradas(limpo: pd.DataFrame) -> pd.Series:
    """Linha só com zeros e no máximo um rótulo de TEXTO (ex.: o produto). Um número
    diferente de zero, mesmo sem identificação, é informação e mantém a linha."""
    zero = limpo.apply(lambda s: s.str.match(_ZERO))
    texto = (limpo != "") & ~zero
    numero = texto & limpo.apply(lambda s: s.str.match(r"^-?[\d.,]+$"))
    return (texto.sum(axis=1) <= 1) & (numero.sum(axis=1) == 0) & (zero.sum(axis=1) >= 2)


def separar_descartes(
    df: pd.DataFrame, refs: List[List[str]], zeradas: bool = False, totais: Iterable[int] = ()
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separa linhas vazias, cópias de cabeçalho, totais, subtítulos, rodapé e, quando o
    mapa pede, as linhas-modelo zeradas e os totais sem rótulo (pela `_linha_origem`)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    if df.empty or not cols:
        return df, _vazio_descartes()
    limpo = df[cols].apply(lambda s: s.fillna("").str.strip())
    cheia = limpo != ""
    n_cheias = cheia.sum(axis=1)
    primeira = limpo.where(cheia).bfill(axis=1).iloc[:, 0].fillna("")
    motivo = pd.Series("", index=df.index)
    motivo[limpo.isin(["", "-"]).all(axis=1)] = VAZIA
    if zeradas:
        motivo[(motivo == "") & _zeradas(limpo)] = ZERADA
    motivo[(motivo == "") & _copias_de_cabecalho(df, cols, refs)] = COPIA_CABECALHO
    total = primeira.str.replace("\xa0", " ").str.lower().str.contains(_TOTAL)
    motivo[(motivo == "") & total] = TOTAL
    totais = list(totais)
    if totais:
        motivo[(motivo == "") & df[LINHA_ORIGEM].isin(totais)] = TOTAL
    if len(cols) >= 4:
        # uma célula só, de texto ou data solta (carimbo de posição); número sozinho fica
        texto = ~primeira.str.match(_NUMERO) | primeira.str.match(_DATA_SOLTA)
        motivo[(motivo == "") & (n_cheias == 1) & texto] = SUBTITULO
        _marcar_rodape(motivo, n_cheias)
    sai = motivo != ""
    return df[~sai].reset_index(drop=True), _descartes(df[sai], cols, motivo[sai])


def remover_colunas_vazias(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """(dados, removidas, vazias mantidas). Só sai a coluna vazia SEM nome (preenchimento
    do Excel); a com nome fica, para guardar o rastro do que a fonte tinha."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    vazias = [c for c in cols if not (df[c].fillna("").str.strip() != "").any()]
    removidas = [c for c in vazias if _SEM_NOME.match(c)]
    return df.drop(columns=removidas), removidas, [c for c in vazias if c not in removidas]


def _cheias(df: pd.DataFrame) -> int:
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    return int(sum((df[c].fillna("").str.strip() != "").sum() for c in cols))


def _cheias_descartes(descartes: pd.DataFrame) -> int:
    return int(sum(sum(v.strip() != "" for v in vals) for vals in descartes["valores"]))


def conferir(entrada: pd.DataFrame, resultados: List[Resultado]) -> None:
    """Nada se perde e nada se inventa: linhas e células preenchidas da entrada = 2 x
    (saída + descartes), e nenhuma linha da saída repete o cabeçalho final."""
    n_saida = sum(len(r.dados) for r in resultados)
    n_desc = sum(len(r.descartes) for r in resultados)
    if len(entrada) != 2 * (n_saida + n_desc):
        raise ErroHistorico(
            f"linhas não fecham: entrada {len(entrada)} != 2 x (saída {n_saida} + descartes {n_desc})"
        )
    cheias_entrada = _cheias(entrada)
    cheias_saida = sum(_cheias(r.dados) + _cheias_descartes(r.descartes) for r in resultados)
    if cheias_entrada != 2 * cheias_saida:
        raise ErroHistorico(
            f"células preenchidas não fecham: entrada {cheias_entrada} != 2 x {cheias_saida}"
        )
    for r in resultados:
        cols = [c for c in r.dados.columns if c != LINHA_ORIGEM]
        if len(r.dados) and _copias_de_cabecalho(r.dados, cols, [cols]).any():
            raise ErroHistorico("linha igual ao cabeçalho ficou na saída")


def dividir_coluna(df: pd.DataFrame, nomes: List[str]) -> pd.DataFrame:
    """Separa a coluna única com os campos colados por "|" (tabelas `tab_*` do BB de 2013).
    Toda linha tem o mesmo número de campos e juntar as partes devolve o texto original."""
    if df.shape[1] != 1:
        raise ErroHistorico(f"dividir: esperava 1 coluna, há {df.shape[1]}")
    texto = df.iloc[:, 0].fillna("")
    erradas = int((texto.str.count(r"\|") != len(nomes) - 1).sum())
    if erradas:
        raise ErroHistorico(f"dividir: {erradas} linha(s) sem {len(nomes)} campos")
    partes = texto.str.split("|", n=len(nomes) - 1, expand=True, regex=False)
    if not (partes.agg("|".join, axis=1) == texto).all():
        raise ErroHistorico("dividir: juntar as partes não devolve o texto original")
    partes.columns, _ = normalizar_colunas(nomes)
    return partes


def _tratar_bloco(unico: pd.DataFrame, regra: Regra, n_entrada: int) -> Resultado:
    dados, consumidas, refs = aplicar_cabecalho(unico, regra)
    dados, outros = separar_descartes(dados, refs, regra.zeradas, regra.totais)
    dados, removidas, vazias = remover_colunas_vazias(dados)
    descartes = pd.concat([consumidas, outros]).sort_values(LINHA_ORIGEM).reset_index(drop=True)
    return Resultado(dados, descartes, removidas, n_entrada, vazias)


def tratar_blocos(df: pd.DataFrame, regras: List[Regra]) -> List[Tuple[str, Resultado]]:
    """Os passos 1-5 sobre as colunas de dado de um arquivo. Com mais de uma regra, cada
    bloco vai de onde o anterior terminou até o seu `fim`. Devolve [(bloco, resultado)]."""
    regras = sorted(regras, key=lambda r: r.linha)
    if len(regras) > 1 and any(r.fim is None for r in regras[:-1]):
        raise ErroHistorico("arquivo com blocos: todo bloco, menos o último, precisa de fim")
    entrada = df.reset_index(drop=True)
    if any(r.acao == DIVIDIR for r in regras):
        if len(regras) != 1:
            raise ErroHistorico("dividir não combina com blocos")
        entrada = dividir_coluna(entrada, regras[0].colunas)  # conferência já dividida
        regras = [
            Regra(
                MANTER,
                0,
                regras[0].motivo,
                regras[0].bloco,
                zeradas=regras[0].zeradas,
                totais=regras[0].totais,
            )
        ]
    unico = desfazer_duplicacao(entrada)
    saida, ini = [], 0
    for regra in regras:
        fim = len(unico) if regra.fim is None else regra.fim
        if not ini <= regra.linha < fim <= len(unico):
            raise ErroHistorico(f"bloco {regra.bloco!r} fora do arquivo: {ini}..{fim}")
        relativa = Regra(
            regra.acao,
            regra.linha - ini,
            regra.motivo,
            regra.bloco,
            colunas=regra.colunas,
            zeradas=regra.zeradas,
            totais=regra.totais,
        )
        saida.append((regra.bloco, _tratar_bloco(unico.iloc[ini:fim], relativa, len(entrada))))
        ini = fim
    if ini != len(unico):
        raise ErroHistorico(f"linhas {ini}..{len(unico)} não pertencem a nenhum bloco")
    _conferir_totais(regras, saida)
    conferir(entrada, [r for _, r in saida])
    return saida


def _conferir_totais(regras: List[Regra], saida: List[Tuple[str, Resultado]]) -> None:
    """Toda linha de total apontada no mapa tem de ter saído como total (número errado no
    mapa não pode passar calado)."""
    pedidas = {n for r in regras for n in r.totais}
    if not pedidas:
        return
    achadas = {
        int(n)
        for _, res in saida
        for n in res.descartes.loc[res.descartes.motivo == TOTAL, LINHA_ORIGEM]
    }
    if pedidas - achadas:
        raise ErroHistorico(f"total do mapa não encontrado nas linhas {sorted(pedidas - achadas)}")


def tratar_arquivo(df: pd.DataFrame, regra: Optional[Regra] = None) -> Resultado:
    """Um arquivo com uma tabela só (o caso comum)."""
    return tratar_blocos(df, [regra or Regra()])[0][1]


# ---------------------------------------------------------------------------------------
# 3a. padronização: um jeito só de escrever cada valor, sem mudar o que ele diz
# ---------------------------------------------------------------------------------------
# Continua tudo texto. Por coluna de cada arquivo: tira o espaço das pontas, conserta o
# mojibake e esvazia os nulos escritos; se a coluna é de data, escreve AAAA-MM-DD; se é de
# número, ponto decimal, sem milhar, sem ".0", sem zero à direita e sem ruído de float.
# Toda célula alterada é conferida (o número e a data continuam os mesmos) e contada; o
# texto original continua no analytics.

ESPACO, MOJIBAKE, NULO, MOEDA = "espaco", "mojibake", "nulo", "moeda"
DATA, DATA_INVALIDA = "data", "data_invalida"
PONTO_ZERO, DECIMAL_BR, RUIDO_FLOAT, FORMA_NUMERO = (
    "numero_ponto_zero",
    "numero_decimal_br",
    "numero_ruido_float",
    "numero_forma",
)
CNPJ_ZEROS = "cnpj_zeros"
_NULOS = frozenset(
    {"null", "nan", "none", "nat", "#n/d", "#n/a", "n/d", "-", "--", "---", "—", "–"}
    | {"0000-00-00", "00/00/0000", "0000-00-00 00:00:00", "00:00:00", "00:00:00.000"}
)
_MOJIBAKE_RX = re.compile("Ã[\x80-\xbf]|Â[\x80-\xbf]|â€")
_D_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$")
_D_BR = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?$")
_N_INT = re.compile(r"^-?\d+$")
_N_PONTO = re.compile(r"^-?\d+\.\d+$")
_N_MILHAR_BR = re.compile(r"^-?\d{1,3}(\.\d{3})+$")  # "1.000": milhar no BR, decimal no resto
_N_BR = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d+$|^-?\d+,\d+$")
# "1.737.972,88": ponto de milhar e vírgula decimal juntos só existem no BR, então vale até
# em coluna que mistura as convenções (SNH BB de 2024-06)
_N_BR_MILHAR = re.compile(r"^-?\d{1,3}(\.\d{3})+,\d+$")
_MOEDA = re.compile(r"^R\$\s*(-|-?[\d.,]+)$")  # "R$ 1.008.000,00", "R$ -"
_N_CIENT = re.compile(r"^-?\d+(\.\d+)?[eE][+-]?\d+$")
# ",05": vírgula decimal sem a parte inteira. Não pode ser milhar, então é decimal em
# qualquer convenção (aparece até em coluna de ponto decimal, como no INT040)
_N_FRACAO = re.compile(r"^-?,\d+$")
_CNPJ_COLUNA = re.compile(r"cnpj")
LIMIAR_TIPO = 0.95  # fração dos valores para a coluna ser de data ou de número
CASAS_RUIDO = 7  # decimal com tantas casas ou mais é ruído de float


def _limpar_texto(v: str) -> Tuple[str, Optional[str]]:
    t = v.replace("\xa0", " ").strip()
    regra = ESPACO if t != v else None
    moeda = _MOEDA.match(t)
    if moeda:
        t, regra = moeda.group(1), MOEDA
    if _MOJIBAKE_RX.search(t):
        corrigido = _corrigir_mojibake(t)
        if corrigido != t:
            t, regra = corrigido, MOJIBAKE
    if t and t.lower() in _NULOS:
        return "", NULO
    return t, regra


def _e_data(t: str) -> bool:
    return bool(_D_ISO.match(t) or _D_BR.match(t))


def _e_numero(t: str) -> bool:
    return any(rx.match(t) for rx in (_N_INT, _N_PONTO, _N_MILHAR_BR, _N_BR, _N_CIENT, _N_FRACAO))


def _data_iso(t: str) -> Tuple[str, str]:
    """(valor, regra) de uma data: AAAA-MM-DD (com a hora só se não for 00:00:00)."""
    m = _D_ISO.match(t)
    if m:
        a, mes, d, hh, mm, ss = m.groups()
    else:
        d, mes, a, hh, mm, ss = _D_BR.match(t).groups()
    try:
        dia = date(int(a), int(mes), int(d))
    except ValueError:
        return "", DATA_INVALIDA
    hora = f"{hh}:{mm}:{ss or '00'}" if hh and f"{hh}{mm}{ss or '00'}" != "000000" else ""
    novo = dia.isoformat() + (f" {hora}" if hora else "")
    return novo, (DATA if novo != t else None)


def _convencao(valores: Iterable[str]) -> Optional[str]:
    """ "br" (vírgula decimal), "ponto", "inteiro" (só inteiros e "1.000", que não dá para
    saber se é mil ou um) ou None quando a coluna mistura vírgula e ponto decimal."""
    br = ponto = False
    for t in valores:
        br = br or bool(_N_BR.match(t))
        ponto = ponto or bool(_N_PONTO.match(t) and not _N_MILHAR_BR.match(t))
    if br and ponto:
        return None
    return "br" if br else ("ponto" if ponto else "inteiro")


def _sem_ruido(texto: str) -> str:
    """A forma curta de um decimal com ruído de float (float32 ou float64)."""
    import numpy as np

    x = float(texto)
    f32 = np.float32(x)
    if float(f32) == x:
        curto = np.format_float_positional(f32, unique=True, trim="-")
    else:
        curto = format(x, ".15g")
    return format(Decimal(curto).normalize(), "f")


def _numero_canonico(t: str, convencao: str) -> Tuple[str, Optional[str]]:
    """(valor, regra) de um número: ponto decimal, sem milhar, sem zero à direita."""
    regra = None
    s = t
    if _N_FRACAO.match(t):
        s, regra = t.replace(",", "."), DECIMAL_BR
    elif convencao == "br" and (_N_BR.match(t) or _N_MILHAR_BR.match(t)):
        s, regra = t.replace(".", "").replace(",", "."), DECIMAL_BR
    d = Decimal(s)
    if d == d.to_integral_value():
        novo = str(int(d))
        if regra is None and novo != t:
            regra = PONTO_ZERO if re.fullmatch(r"-?\d+\.0+", t) else FORMA_NUMERO
        return novo, regra
    casas = len(s.split(".")[1]) if "." in s and "e" not in s.lower() else 0
    if casas >= CASAS_RUIDO:
        novo = _sem_ruido(s)
        return novo, (RUIDO_FLOAT if novo != t else regra)
    novo = format(d.normalize(), "f")
    return novo, (regra or (FORMA_NUMERO if novo != t else None))


def _conferir_numero(antes: str, depois: str, convencao: str, coluna: str) -> None:
    s = antes.replace(".", "").replace(",", ".") if convencao == "br" and "," in antes else antes
    if convencao == "br" and _N_MILHAR_BR.match(antes):
        s = antes.replace(".", "")
    if _N_FRACAO.match(antes):
        s = antes.replace(",", ".")
    a, b = float(s), float(depois)
    if abs(a - b) > 1e-6 * max(1.0, abs(a)):
        raise ErroHistorico(f"padronização mudou o número em {coluna}: {antes!r} -> {depois!r}")


def _tipo_coluna(contagem: Dict[str, int]) -> str:
    cheias = {t: n for t, n in contagem.items() if t}
    total = sum(cheias.values())
    if not total:
        return "texto"
    if sum(n for t, n in cheias.items() if _e_data(t)) >= LIMIAR_TIPO * total:
        return "data"
    if sum(n for t, n in cheias.items() if _e_numero(t)) >= LIMIAR_TIPO * total:
        return "numero"
    return "texto"


def _mapa_numerico(coluna: str, limpos: Dict[str, str]) -> Dict[str, Tuple[str, Optional[str]]]:
    numeros = [t for t in set(limpos.values()) if t and _e_numero(t)]
    if any(re.match(r"^-?0\d", t) for t in numeros):
        # código com zero à esquerda (CEP, código): só sai o ".0"
        return {
            t: (re.sub(r"\.0+$", "", t), PONTO_ZERO)
            for t in numeros
            if re.fullmatch(r"-?\d+\.0+", t)
        }
    convencao = _convencao(numeros)
    if convencao is None:
        log.warning("%s mistura vírgula e ponto decimal: só o milhar BR inequívoco muda", coluna)
        mapa = {}
        for t in numeros:
            if _N_BR_MILHAR.match(t):
                novo, regra = _numero_canonico(t, "br")
                _conferir_numero(t, novo, "br", coluna)
                mapa[t] = (novo, regra)
        return mapa
    mapa = {}
    for t in numeros:
        if convencao == "inteiro" and _N_MILHAR_BR.match(t):
            continue  # ambíguo: fica como está
        novo, regra = _numero_canonico(t, convencao)
        if regra:
            _conferir_numero(t, novo, convencao, coluna)
            mapa[t] = (novo, regra)
    return mapa


def padronizar_coluna(coluna: str, serie: pd.Series) -> Tuple[pd.Series, Dict[str, int], Dict]:
    """(série padronizada, células por regra, exemplo antes -> depois por regra)."""
    unicos = serie.fillna("").value_counts().to_dict()
    passo1 = {v: _limpar_texto(v) for v in unicos}
    limpos = {v: t for v, (t, _) in passo1.items()}
    final = dict(passo1)
    tipo = _tipo_coluna(_somar(limpos, unicos))
    if tipo == "data":
        segundo = {t: _data_iso(t) for t in set(limpos.values()) if t and _e_data(t)}
    elif tipo == "numero":
        segundo = _mapa_numerico(coluna, limpos)
    else:
        segundo = {}
    for v, t in limpos.items():
        if t in segundo and segundo[t][1]:
            final[v] = segundo[t]
    if _CNPJ_COLUNA.search(coluna):
        for v, (t, _regra) in final.items():
            if re.fullmatch(r"\d{11,13}", t):
                final[v] = (t.zfill(14), CNPJ_ZEROS)
    contagem: Dict[str, int] = {}
    exemplos: Dict[str, Tuple[str, str]] = {}
    for v, (t, regra) in final.items():
        if regra and t != v:
            contagem[regra] = contagem.get(regra, 0) + unicos[v]
            exemplos.setdefault(regra, (v, t))
    mapa = {v: t for v, (t, regra) in final.items() if regra and t != v}
    return (serie.map(mapa).fillna(serie) if mapa else serie), contagem, exemplos


def _somar(limpos: Dict[str, str], unicos: Dict[str, int]) -> Dict[str, int]:
    soma: Dict[str, int] = {}
    for v, t in limpos.items():
        soma[t] = soma.get(t, 0) + unicos[v]
    return soma


def padronizar(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[dict]]:
    """Padroniza todas as colunas de dado. Devolve (dados, [{coluna, regra, celulas,
    antes, depois}]). As células preenchidas só diminuem pelos nulos e datas inválidas."""
    saida = df.copy()
    registros = []
    for c in [c for c in df.columns if c != LINHA_ORIGEM]:
        saida[c], contagem, exemplos = padronizar_coluna(c, df[c])
        for regra, n in contagem.items():
            antes, depois = exemplos[regra]
            registros.append(dict(coluna=c, regra=regra, celulas=n, antes=antes, depois=depois))
    esvaziadas = sum(r["celulas"] for r in registros if r["regra"] in (NULO, DATA_INVALIDA))
    if _cheias(saida) != _cheias(df) - esvaziadas:
        raise ErroHistorico(
            "padronização mudou células preenchidas além dos nulos e datas inválidas"
        )
    return saida, registros


def remover_duplicadas(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Linha idêntica repetida (fora `_linha_origem`): fica a primeira. Só vale em família
    com chave (APF, código, contrato), onde a repetição é registro duplicado na fonte."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    repetida = df.duplicated(cols, keep="first")
    return df[~repetida].reset_index(drop=True), _descartes(df[repetida], cols, DUPLICADA)


def hash_conteudo(df: pd.DataFrame) -> str:
    """md5 do conteúdo, independente da ordem das linhas (sem `_linha_origem`)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    linhas = pd.util.hash_pandas_object(df[cols], index=False).sort_values()
    return hashlib.md5(("|".join(cols) + ":" + ",".join(map(str, linhas))).encode()).hexdigest()


# ---------------------------------------------------------------------------------------
# 3b. datas: competência (mês a que o relatório se refere) e data de corte (extração)
# ---------------------------------------------------------------------------------------
# O nome do arquivo não basta: é `<pasta>_<nome original>`, a pasta nem sempre é o mês do
# dado e nomes com mais de 63 caracteres perdem a data do fim. Regras, nesta ordem:
# 1. `data_de_movimento` constante (SNH) é a competência e o corte;
# 2. lote de entrega do BB ao MCid (pf_pf, pj e pj_pf do mesmo dia, 2014-2019): mês do
#    contrato mais novo do pf_pf, ou o anterior se ele cai até o dia 12 (o `dat_atualizacao`
#    desses arquivos não é confiável);
# 3. data no nome original (a última com dia, senão a última com mês);
# 4. só o mês da pasta.
# O corte nunca fica antes do contrato mais novo. Conflitos viram alertas, não mudam nada.

COLUNAS_CONTRATO = (
    "dat_contratacao",
    "data_da_contratacao_bb",
    "data_de_contratacao",
    "data_contratacao",
    "dt_contratacao",
    "data_de_contratacao_convenio",
    "dte_assin_contrato",
    "dte_assin_contrat",  # nome cortado em 63 caracteres
)
COLUNAS_DECLARADAS = ("data_de_movimento", "dat_atualizacao", "data_pos_arq")
DECLARADA_CONFIAVEL = "data_de_movimento"
INICIO = date(2008, 1, 1)
DIA_LIMITE_FECHAMENTO = 12
DIA_FIM_DO_MES = 26
DIAS_CONTRATO_ANTIGO = 60
FONTE_MOVIMENTO, FONTE_LOTE = "data_de_movimento", "lote_contrato_mais_novo"
FONTE_NOME, FONTE_PASTA, FONTE_CONTRATO = "nome_arquivo", "pasta", "contrato_mais_novo"

_NOMES_MES = (
    "janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro"
)
_PASTA = re.compile(rf"(?:^|_)((?:19|20)\d{{2}})_(0[1-9]|1[0-2])_+(?:{_NOMES_MES})_+(.*)$")
_A4 = r"(?P<a>(?:19|20)\d{2})"
_A2 = r"(?P<a>(?:19|20)?\d{2})"
_M = r"(?P<m>0[1-9]|1[0-2])"
_D = r"(?P<d>0[1-9]|[12]\d|3[01])"
_MX = r"(?P<mx>" + "|".join(_MESES) + r")[a-zç]*"
_PADROES_DATA = (
    (re.compile(rf"(?<!\d){_A4}{_M}{_D}(?!\d)"), "dia"),  # 20131105
    (re.compile(rf"(?<!\d){_D}{_M}{_A4}(?!\d)"), "dia"),  # 31122016
    (re.compile(rf"(?<!\d){_A4}_{_M}_{_D}(?!\d)"), "dia"),  # 2012_04_18
    (re.compile(rf"(?<!\d){_D}_{_M}_{_A2}(?!\d)"), "dia"),  # 08_05_2012, 31_07_12
    (re.compile(rf"(?<!\d){_D}_?{_MX}_?{_A2}(?!\d)"), "dia"),  # 18abr11, 03_jan_12
    (re.compile(rf"(?<!\d){_D}{_M}(?P<a>0[89]|1\d|2[0-6])(?!\d)"), "dia"),  # 150513
    (re.compile(rf"(?<![a-z\d]){_MX}_?{_A2}(?!\d)"), "mes"),  # fev2017, mai18
    (re.compile(rf"(?<!\d){_A4}_?{_M}(?!\d)"), "mes"),  # 202411, 2024_10
)
_CORTADO = re.compile(rf"(?<!\d){_D}{_M}(?P<a3>(?:19|20)\d)$")  # "1610201" no fim do nome
_DIA_MES = re.compile(rf"(?<!\d){_D}{_M}(?!\d)")  # "3011", com o ano da pasta
_DIAS_DA_PASTA = 62
_LOTE_MIN_CIDADES = re.compile(r"^(.+?)_min_cidades_(pf_pf|pj_pf|pj)(?:_\d{4}_\d{2})?$")
_LOTE_BB = re.compile(r"^(bb_20(?:18|19)_.+?)_(pf_pf|pj_pf|pj)$")
_PREFERENCIA_LOTE = ("pf_pf", "pj_pf", "pj")


def _data(ano, mes, dia) -> Optional[date]:
    ano = int(ano)
    try:
        return date(ano + 2000 if ano < 100 else ano, int(mes), int(dia))
    except ValueError:
        return None


def _mes(d: Optional[date]) -> Optional[str]:
    return f"{d.year:04d}-{d.month:02d}" if d else None


def _mes_anterior(d: date) -> str:
    return _mes(d.replace(day=1) - timedelta(days=1))


def _datas_completas(texto: str, hoje: date) -> List[tuple]:
    achadas = []
    for rx, precisao in _PADROES_DATA:
        for a in rx.finditer(texto):
            g = a.groupdict()
            mes = _MESES.index(g["mx"]) + 1 if g.get("mx") else g["m"]
            d = _data(g["a"], mes, g.get("d") or 1)
            if d and INICIO <= d <= hoje:
                achadas.append((a.start(), a.end(), d, precisao))
    return achadas


def _datas_com_ano_da_pasta(texto: str, pasta: date, so_cortadas: bool) -> List[tuple]:
    achadas = []
    for rx in (_CORTADO,) if so_cortadas else (_CORTADO, _DIA_MES):
        for a in rx.finditer(texto):
            if rx is _CORTADO and not str(pasta.year).startswith(a.group("a3")):
                continue
            d = _data(pasta.year, a.group("m"), a.group("d"))
            if d and abs((d - pasta).days) <= _DIAS_DA_PASTA:
                achadas.append((a.start(), a.end(), d, "dia"))
    return achadas


def data_do_nome(nome: str, hoje: Optional[date] = None) -> Tuple[Optional[date], Optional[str]]:
    """(data, precisão "dia" | "mes") do nome original; sem data nele, o mês da pasta com
    precisão "pasta". Vale a última data com dia, senão a última com mês."""
    hoje = hoje or date.today()
    pasta, original = None, nome
    m = _PASTA.search(nome)
    if m:
        pasta, original = date(int(m.group(1)), int(m.group(2)), 1), m.group(3)
    achadas = _datas_completas(original, hoje)
    if pasta:
        achadas += _datas_com_ano_da_pasta(original, pasta, so_cortadas=bool(achadas))
    achadas.sort(key=lambda x: (x[0], x[0] - x[1]))
    finais = []
    for a in achadas:  # um achado dentro de outro maior não conta
        if not any(b[0] <= a[0] and a[1] <= b[1] for b in finais):
            finais.append(a)
    if finais:
        com_dia = [a for a in finais if a[3] == "dia"]
        escolhida = max(com_dia or finais, key=lambda x: x[0])
        return escolhida[2], escolhida[3]
    return (pasta, FONTE_PASTA) if pasta else (None, None)


def mes_da_pasta(nome: str) -> Optional[str]:
    m = _PASTA.search(nome)
    return f"{m.group(1)}-{m.group(2)}" if m else None


def para_datas(valores: Iterable) -> pd.Series:
    """AAAA-MM-DD ou DD/MM/AAAA (com ou sem hora) -> datetime; o resto vira NaT."""
    s = pd.Series(list(valores), dtype="string")
    iso = s.str.extract(r"^\s*(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})")
    br = s.str.extract(r"^\s*(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})")
    partes = iso.fillna(br).astype("float")
    if partes.empty:
        return pd.Series([], dtype="datetime64[ns]")
    validas = partes.notna().all(axis=1)
    saida = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if validas.any():
        saida[validas] = pd.to_datetime(partes[validas], errors="coerce")
    return saida


def evidencias(df: pd.DataFrame, hoje: Optional[date] = None) -> dict:
    """O que o conteúdo diz sobre a data: o contrato mais novo e a data declarada."""
    hoje = hoje or date.today()
    ev = {
        "contrato_coluna": None,
        "contrato_mais_novo": None,
        "data_declarada_coluna": None,
        "data_declarada": None,
        "data_declarada_distintas": 0,
    }
    coluna = next((c for c in COLUNAS_CONTRATO if c in df.columns), None)
    if coluna:
        datas = para_datas(df[coluna].dropna().unique())
        datas = datas[(datas >= pd.Timestamp(INICIO)) & (datas <= pd.Timestamp(hoje))]
        ev["contrato_coluna"] = coluna
        ev["contrato_mais_novo"] = datas.max().date() if len(datas) else None
    coluna = next((c for c in COLUNAS_DECLARADAS if c in df.columns), None)
    if coluna:
        valores = sorted({str(v).strip() for v in df[coluna].dropna().unique()} - {""})
        ev["data_declarada_coluna"] = coluna
        ev["data_declarada_distintas"] = len(valores)
        ev["data_declarada"] = valores[0] if len(valores) == 1 else None
    return ev


def lote(arquivo: str) -> Tuple[Optional[str], Optional[str]]:
    """(lote, sub-arquivo) de um arquivo da entrega do BB ao MCid; fora dela, (None, None)."""
    m = _LOTE_MIN_CIDADES.match(arquivo) or _LOTE_BB.match(arquivo)
    if not m:
        return None, None
    return re.sub(r"(\d{4}_\d{2}_\d{2})_\1$", r"\1", m.group(1)), m.group(2)


def _referencia_do_lote(membros: List[Tuple[str, dict]]) -> Optional[Tuple[date, str, date]]:
    for preferido in _PREFERENCIA_LOTE:
        datas = [r["contrato_mais_novo"] for sub, r in membros if sub == preferido]
        datas = [d for d in datas if d]
        if datas:
            todas = [r["contrato_mais_novo"] for _, r in membros if r["contrato_mais_novo"]]
            return max(datas), preferido, max(todas)
    return None


def datar(registros: List[dict], hoje: Optional[date] = None) -> None:
    """Competência, corte, fontes e alertas em cada registro do log do cleansed."""
    hoje = hoje or date.today()
    for r in registros:
        for c in ("contrato_mais_novo", "data_declarada", "data_declarada_coluna"):
            r.setdefault(c, None)
    lotes: Dict[str, List[Tuple[str, dict]]] = {}
    for r in registros:
        r["lote"], sub = lote(r["arquivo"])
        if r["lote"]:
            lotes.setdefault(r["lote"], []).append((sub, r))
    referencias = {nome: _referencia_do_lote(membros) for nome, membros in lotes.items()}
    for r in registros:
        _datar_um(r, referencias.get(r["lote"]), hoje)


def _data_declarada(r: dict, alertas: List[str]) -> Optional[date]:
    if r.get("data_declarada_distintas", 0) > 1:
        alertas.append("data_declarada_varia")
    if not r["data_declarada"]:
        return None
    d = para_datas([r["data_declarada"]]).iloc[0]
    if pd.isna(d):
        alertas.append("data_declarada_invalida")
        return None
    return d.date()


def _competencia_do_lote(referencia_lote: tuple, alertas: List[str]) -> str:
    referencia, sub, _ = referencia_lote
    if sub != _PREFERENCIA_LOTE[0]:
        alertas.append("lote_sem_pf_pf")
    if referencia.day <= DIA_LIMITE_FECHAMENTO:
        return _mes_anterior(referencia)
    if referencia.day < DIA_FIM_DO_MES:
        alertas.append("corte_no_meio_do_mes")
    return _mes(referencia)


def _alertas_da_competencia(r: dict, competencia, fonte, declarada, alertas) -> None:
    if competencia is None:
        alertas.append("sem_competencia")
        return
    if fonte in (FONTE_MOVIMENTO, FONTE_LOTE) and r["data_nome"]:
        if competencia != _mes(r["data_nome"]):
            alertas.append("competencia_diferente_do_nome")
    inicio = date(int(competencia[:4]), int(competencia[5:]), 1)
    contrato = r["contrato_mais_novo"]
    if contrato and (inicio - contrato).days > DIAS_CONTRATO_ANTIGO:
        alertas.append("contrato_mais_novo_antigo")
    if declarada and fonte != FONTE_MOVIMENTO and _mes(declarada) != competencia:
        alertas.append("data_declarada_diferente")


def _datar_um(r: dict, referencia_lote: Optional[Tuple[date, str, date]], hoje: date) -> None:
    alertas: List[str] = []
    data_nome, precisao = data_do_nome(r["arquivo"], hoje)
    r["data_nome"], r["data_nome_precisao"] = data_nome, precisao
    pasta = mes_da_pasta(r["arquivo"])
    if pasta and precisao != FONTE_PASTA and data_nome and _mes(data_nome) != pasta:
        alertas.append("pasta_diferente_do_nome")
    declarada = _data_declarada(r, alertas)
    competencia = fonte = corte = fonte_corte = None
    contrato = r["contrato_mais_novo"]
    if declarada and r["data_declarada_coluna"] == DECLARADA_CONFIAVEL:
        competencia, fonte = _mes(declarada), FONTE_MOVIMENTO
        corte, fonte_corte = declarada, FONTE_MOVIMENTO
    elif referencia_lote:
        competencia, fonte = _competencia_do_lote(referencia_lote, alertas), FONTE_LOTE
        contrato = referencia_lote[2]  # o lote tem um corte só
    elif data_nome:
        competencia = _mes(data_nome)
        fonte = FONTE_PASTA if precisao == FONTE_PASTA else FONTE_NOME
    _alertas_da_competencia(r, competencia, fonte, declarada, alertas)
    if corte is None and precisao == "dia":
        corte, fonte_corte = data_nome, FONTE_NOME
    if corte and contrato and contrato > corte:
        alertas.append("contrato_depois_do_corte")
        corte, fonte_corte = contrato, FONTE_CONTRATO
    r.update(
        competencia=competencia,
        competencia_fonte=fonte,
        data_corte=corte,
        data_corte_fonte=fonte_corte,
        alertas_data=alertas,
    )


# ---------------------------------------------------------------------------------------
# 3c. cleansed: plano, famílias, log
# ---------------------------------------------------------------------------------------

LOG_CLEANSED = CLEANSED + "_cleansed_log.parquet"
DESCARTES_CLEANSED = CLEANSED + "_cleansed_descartes.parquet"
EVOLUCAO = CLEANSED + "_colunas_evolucao.parquet"
CANDIDATOS = CLEANSED + "_colunas_candidatos.parquet"
CONCEITOS_ARQ = CLEANSED + "_colunas_conceitos.parquet"
GRAFO = CLEANSED + "_grafo_colunas.md"
PADRONIZACOES = CLEANSED + "_cleansed_padronizacoes.parquet"
# versão substituída fica no cleansed, fora da pasta da família (a bronze não lê)
SUBSTITUIDAS = CLEANSED + "_versoes_substituidas/"
# arquivos de controle do cleansed; tudo o mais é saída (há saída que começa com "_")
CONTROLE_CLEANSED = frozenset(
    {LOG_CLEANSED, DESCARTES_CLEANSED, EVOLUCAO, CANDIDATOS, CONCEITOS_ARQ, PADRONIZACOES}
)
LIMITE_UNNAMED = 0.5  # a partir daqui, arquivo fora do mapa está com o cabeçalho errado
TRATADO, EXCLUIDO, SUBSTITUIDA = "tratado", "excluido_formatado", "versao_substituida"
CONTEUDO_IDENTICO, VERSAO_CORRIGIDA = "conteudo_identico", "versao_corrigida"

SCHEMA_CLEANSED = pa.schema(
    [
        ("arquivo", pa.string()),
        ("bloco", pa.string()),
        ("saida", pa.string()),
        ("familia", pa.string()),
        ("chave_familia", pa.list_(pa.string())),
        ("analytics_key", pa.string()),
        ("cleansed_key", pa.string()),
        ("status", pa.string()),
        ("periodo", pa.string()),
        ("competencia", pa.string()),
        ("competencia_fonte", pa.string()),
        ("data_corte", pa.date32()),
        ("data_corte_fonte", pa.string()),
        ("alertas_data", pa.list_(pa.string())),
        ("data_nome", pa.date32()),
        ("data_nome_precisao", pa.string()),
        ("lote", pa.string()),
        ("contrato_coluna", pa.string()),
        ("contrato_mais_novo", pa.date32()),
        ("data_declarada_coluna", pa.string()),
        ("data_declarada", pa.string()),
        ("cabecalho_acao", pa.string()),
        ("cabecalho_linha", pa.int64()),
        ("bloco_fim", pa.int64()),
        ("cabecalho_colunas", pa.list_(pa.string())),
        ("cabecalho_motivo", pa.string()),
        ("descartar_zeradas", pa.bool_()),
        ("linhas_total", pa.list_(pa.int64())),
        ("linhas_entrada", pa.int64()),
        ("linhas_reais", pa.int64()),
        ("linhas_descartadas", pa.int64()),
        ("descartes_por_motivo", pa.map_(pa.string(), pa.int64())),
        ("colunas_entrada", pa.list_(pa.string())),
        ("colunas_removidas", pa.list_(pa.string())),
        ("colunas_vazias", pa.list_(pa.string())),
        ("n_colunas", pa.int64()),
        ("colunas", pa.list_(pa.string())),
        ("padronizacoes", pa.map_(pa.string(), pa.int64())),  # células alteradas por regra
        ("hash_conteudo", pa.string()),  # do conteúdo padronizado
        ("substituida_por", pa.string()),
        ("motivo_substituicao", pa.string()),
        ("gerado_em", pa.string()),
    ]
)
SCHEMA_PADRONIZACOES = pa.schema(
    [
        ("saida", pa.string()),
        ("coluna", pa.string()),
        ("regra", pa.string()),
        ("celulas", pa.int64()),
        ("antes", pa.string()),  # um exemplo
        ("depois", pa.string()),
    ]
)
SCHEMA_DESCARTES = pa.schema(
    [
        ("arquivo", pa.string()),
        ("bloco", pa.string()),
        (LINHA_ORIGEM, pa.int64()),
        ("motivo", pa.string()),
        ("valores", pa.list_(pa.string())),
    ]
)


def ler_mapa(mapa: pd.DataFrame) -> Dict[str, List[Regra]]:
    """`cabecalhos.csv`: arquivo, bloco, acao, linha, fim, colunas ("|"), motivo,
    linhas_zeradas ("descartar" descarta as linhas-modelo zeradas do arquivo) e linhas_total
    (`_linha_origem` de totais sem rótulo, "|"; a linha sai como total). A ação vazia mantém
    o cabeçalho (serve para pedir só esses descartes)."""
    regras: Dict[str, List[Regra]] = {}
    for r in mapa.itertuples():
        if r.acao not in (PROMOVER, COMPOR, NOMEAR, DIVIDIR, EXCLUIR, MANTER):
            raise ErroHistorico(f"mapa de cabeçalhos: ação inválida {r.acao!r} para {r.arquivo}")
        colunas = r.colunas.split("|") if r.colunas else []
        if r.acao in (NOMEAR, DIVIDIR) and not colunas:
            raise ErroHistorico(f"mapa de cabeçalhos: {r.acao} sem colunas para {r.arquivo}")
        regra = Regra(
            r.acao,
            int(r.linha) if r.linha else 0,
            r.motivo,
            r.bloco,
            int(r.fim) if r.fim else None,
            colunas,
            getattr(r, "linhas_zeradas", "") == "descartar",
            [int(n) for n in str(getattr(r, "linhas_total", "") or "").split("|") if n],
        )
        regras.setdefault(r.arquivo, []).append(regra)
    for arquivo, rs in regras.items():
        blocos = [r.bloco for r in rs]
        if len(rs) > 1 and (not all(blocos) or len(set(blocos)) != len(blocos)):
            raise ErroHistorico(f"mapa de cabeçalhos: {arquivo} tem blocos sem nome ou repetidos")
    return regras


def ler_familias(familias: pd.DataFrame) -> List[Tuple[str, re.Pattern, List[str]]]:
    """`familias.csv`: familia, padrao (regex sobre a saída), chave (colunas que identificam a
    linha, separadas por "|"; vazia quando a família não tem chave), fonte, descricao."""
    return [
        (r.familia, re.compile(r.padrao), [c for c in getattr(r, "chave", "").split("|") if c])
        for r in familias.itertuples()
    ]


def chaves_da_familia(familia: Optional[str], familias) -> List[str]:
    return next((chaves for nome, _, chaves in familias if nome == familia), [])


def familia_de(saida: str, familias) -> Optional[str]:
    """A família da saída; erro se casar com mais de uma; None se com nenhuma."""
    achadas = [nome for nome, padrao, _ in familias if padrao.search(saida)]
    if len(achadas) > 1:
        raise ErroHistorico(f"{saida} casa com mais de uma família: {achadas}")
    return achadas[0] if achadas else None


def nome_saida(arquivo: str, bloco: str) -> str:
    return f"{arquivo}__{bloco}" if bloco else arquivo


def _fracao_unnamed(colunas: List[str]) -> float:
    return sum(c.startswith("unnamed") for c in colunas) / max(len(colunas), 1)


def _registro(s: dict, regra: Regra, familias) -> dict:
    saida = nome_saida(s["arquivo"], regra.bloco)
    familia = familia_de(saida, familias)
    return {
        "arquivo": s["arquivo"],
        "bloco": regra.bloco or None,
        "saida": saida,
        "familia": familia,
        "chave_familia": chaves_da_familia(familia, familias) or None,
        "descartar_zeradas": regra.zeradas,
        "linhas_total": regra.totais or None,
        "analytics_key": s["analytics_key"],
        "periodo": s.get("periodo"),
        "colunas_entrada": list(s["colunas"]),
        "status": EXCLUIDO if regra.acao == EXCLUIR else TRATADO,
        "cabecalho_acao": regra.acao,
        "cabecalho_linha": regra.linha if regra.acao in (PROMOVER, COMPOR) else None,
        "cabecalho_colunas": regra.colunas or None,
        "bloco_fim": regra.fim,
        "cabecalho_motivo": regra.motivo or None,
    }


def planejar(selecionados: List[dict], mapa: Dict[str, List[Regra]], familias) -> List[dict]:
    """Um registro por saída (arquivo, ou arquivo + bloco). Falha se um arquivo fora do
    mapa tem o cabeçalho quase todo `unnamed_*`, ou se uma saída tratada não tem família."""
    regs, sem_regra = [], []
    for s in selecionados:
        regras = mapa.get(s["arquivo"], [Regra(MANTER)])
        regs += [_registro(s, regra, familias) for regra in regras]
        if regras[0].acao == MANTER and _fracao_unnamed(list(s["colunas"])) >= LIMITE_UNNAMED:
            sem_regra.append(s["arquivo"])
    if sem_regra:
        raise ErroHistorico(
            f"{len(sem_regra)} arquivo(s) com cabeçalho quase todo unnamed_* e fora do mapa "
            f"de cabeçalhos: {sem_regra[:5]}"
        )
    sem_familia = [r["saida"] for r in regs if r["status"] == TRATADO and not r["familia"]]
    if sem_familia:
        raise ErroHistorico(f"{len(sem_familia)} saída(s) sem família: {sem_familia[:5]}")
    for arquivo in sorted(set(mapa) - {s["arquivo"] for s in selecionados}):
        log.warning("mapa de cabeçalhos: %s não está no analytics", arquivo)
    return regs


def _gravar_saida(dados: pd.DataFrame, linhagem: dict, local: Path) -> None:
    saida = pa.Table.from_pandas(dados, preserve_index=False)
    for c, v in linhagem.items():
        saida = saida.append_column(c, pa.array([v] * saida.num_rows, pa.string()))
    local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(saida, local, compression="snappy")


def _somar_regras(padronizacoes: List[dict]) -> Dict[str, int]:
    total: Dict[str, int] = {}
    for p in padronizacoes:
        total[p["regra"]] = total.get(p["regra"], 0) + p["celulas"]
    return total


def tratar_parquet(
    path: Path,
    regras: List[Regra],
    destino: Optional[Path],
    familias: Dict[str, str],
    chaves: Optional[Dict[str, List[str]]] = None,
) -> Tuple[Dict[str, dict], pd.DataFrame, List[dict]]:
    """Trata um Parquet do analytics: estrutura, padronização e, em família com chave,
    linhas duplicadas. Grava cada bloco em `destino/<familia>/<saida>.parquet` (se dado).
    Devolve ({bloco: campos do log}, descartes, padronizações por coluna)."""
    tabela = pq.read_table(path)
    linhagem = {c: tabela.column(c)[0].as_py() for c in ("_source_file", "_source_hash")}
    dados = tabela.drop([c for c in tabela.column_names if c.startswith("_")]).to_pandas()
    campos, descartes, padronizacoes = {}, [], []
    for bloco, res in tratar_blocos(dados, regras):
        saida_nome = nome_saida(path.stem, bloco)
        ev = evidencias(res.dados)  # a data do arquivo sai do valor original
        padronizado, pads = padronizar(res.dados)
        desc = res.descartes
        if any(c in padronizado.columns for c in (chaves or {}).get(saida_nome, [])):
            padronizado, duplicadas = remover_duplicadas(padronizado)
            desc = pd.concat([desc, duplicadas]).sort_values(LINHA_ORIGEM).reset_index(drop=True)
        if destino:
            _gravar_saida(
                padronizado, linhagem, destino / familias[saida_nome] / f"{saida_nome}.parquet"
            )
        colunas = [c for c in padronizado.columns if c != LINHA_ORIGEM]
        por_motivo = desc.motivo.value_counts().to_dict() if len(desc) else {}
        campos[bloco] = {
            "linhas_entrada": res.linhas_entrada,
            "linhas_reais": len(padronizado),
            "linhas_descartadas": len(desc),
            "descartes_por_motivo": por_motivo,
            "colunas_removidas": res.colunas_removidas,
            "colunas_vazias": res.colunas_vazias,
            "n_colunas": len(colunas),
            "colunas": colunas,
            "padronizacoes": _somar_regras(pads),
            "hash_conteudo": hash_conteudo(padronizado),
            **ev,
        }
        descartes.append(desc.assign(arquivo=path.stem, bloco=bloco or None))
        padronizacoes += [dict(saida=saida_nome, **p) for p in pads]
    return campos, pd.concat(descartes, ignore_index=True), padronizacoes


def escrever_log_cleansed(registros: List[dict], path: Path) -> None:
    linhas = [{c: r.get(c) for c in SCHEMA_CLEANSED.names} for r in registros]
    for linha in linhas:
        for campo in ("descartes_por_motivo", "padronizacoes"):
            if linha[campo] is not None:
                linha[campo] = [(k, int(v)) for k, v in linha[campo].items()]
    pq.write_table(pa.Table.from_pylist(linhas, schema=SCHEMA_CLEANSED), path)


def escrever_descartes(descartes: List[pd.DataFrame], path: Path) -> None:
    if descartes:
        df = pd.concat(descartes, ignore_index=True)[SCHEMA_DESCARTES.names]
        tabela = pa.Table.from_pandas(df, schema=SCHEMA_DESCARTES, preserve_index=False)
    else:
        tabela = SCHEMA_DESCARTES.empty_table()
    pq.write_table(tabela, path)


def _regras_do_registro(regs: List[dict]) -> List[Regra]:
    return [
        Regra(
            r["cabecalho_acao"],
            r["cabecalho_linha"] or 0,
            bloco=r["bloco"] or "",
            fim=r["bloco_fim"],
            colunas=list(r["cabecalho_colunas"] or []),
            zeradas=bool(r.get("descartar_zeradas")),
            totais=list(r.get("linhas_total") or []),
        )
        for r in regs
    ]


def _tratar_todos(
    lake: Lake, registros: List[dict], tmp: Path
) -> Tuple[List[pd.DataFrame], List[dict]]:
    """Trata cada arquivo em `tmp/saida/`. Tenta todos; se algum falhar, erro no fim."""
    por_arquivo: Dict[str, List[dict]] = {}
    for r in registros:
        if r["status"] == TRATADO:
            por_arquivo.setdefault(r["arquivo"], []).append(r)
    familias = {r["saida"]: r["familia"] for r in registros}
    chaves = {r["saida"]: list(r["chave_familia"] or []) for r in registros}
    descartes, padronizacoes, erros = [], [], []
    for i, (arquivo, regs) in enumerate(por_arquivo.items(), 1):
        local = lake.baixar(regs[0]["analytics_key"], tmp / "entrada" / f"{arquivo}.parquet")
        try:
            campos, desc, pads = tratar_parquet(
                local, _regras_do_registro(regs), tmp / "saida", familias, chaves
            )
        except ErroHistorico as exc:
            log.error("[%d/%d] %s: %s", i, len(por_arquivo), arquivo, exc)
            erros.append(arquivo)
            continue
        finally:
            local.unlink()
        for r in regs:
            r.update(campos[r["bloco"] or ""])
            r["cleansed_key"] = f"{CLEANSED}{r['familia']}/{r['saida']}.parquet"
        descartes.append(desc)
        padronizacoes += pads
        if i % 50 == 0:
            log.info("[%d/%d] tratados", i, len(por_arquivo))
    if erros:
        raise ErroHistorico(f"{len(erros)} arquivo(s) falharam no tratamento: {erros[:5]}")
    return descartes, padronizacoes


def rank_versao(saida: str) -> int:
    """Quanto maior, mais corrigida a versão: correção/ajuste > v3 > v2 > original > antigo
    ou a 2ª versão com a acentuação quebrada (`min_cidades_pj_AAAA_MM`)."""
    if re.search(r"correcao|ajustada", saida):
        return 30
    m = re.search(r"(?:^|_|\d)vs?0?(\d)(?:$|_)", saida)
    if m:
        return 10 + int(m.group(1))
    if re.search(r"_antigo$|min_cidades_pj_\d{4}_\d{2}$", saida):
        return 0
    return 10


def _substituir(r: dict, por: dict, motivo: str) -> None:
    r.update(status=SUBSTITUIDA, substituida_por=por["saida"], motivo_substituicao=motivo)
    r["cleansed_key"] = f"{SUBSTITUIDAS}{r['familia']}/{r['saida']}.parquet"


def escolher_versoes(registros: List[dict]) -> int:
    """Uma saída por conteúdo e por versão, dentro da família. Conteúdo idêntico depois da
    padronização: fica o de menor competência (e, no mesmo mês, a versão mais corrigida).
    Mesmo mês, mesmo corte e mesmas colunas com conteúdo diferente: fica a versão mais
    corrigida, se o nome disser qual é. As outras vão para `_versoes_substituidas/`."""
    tratados = [r for r in registros if r["status"] == TRATADO]
    ordem = lambda r: (r["competencia"] or "9999", -rank_versao(r["saida"]), r["saida"])  # noqa: E731
    grupos: Dict[tuple, List[dict]] = {}
    for r in tratados:
        grupos.setdefault((r["familia"], r["hash_conteudo"]), []).append(r)
    for g in grupos.values():
        fica, *saem = sorted(g, key=ordem)
        for r in saem:
            _substituir(r, fica, CONTEUDO_IDENTICO)
    grupos = {}
    for r in (r for r in tratados if r["status"] == TRATADO):
        chave = (r["familia"], r["competencia"], str(r["data_corte"]), tuple(sorted(r["colunas"])))
        grupos.setdefault(chave, []).append(r)
    for g in (g for g in grupos.values() if len(g) > 1):
        g = sorted(g, key=lambda r: (-rank_versao(r["saida"]), r["saida"]))
        if rank_versao(g[0]["saida"]) == rank_versao(g[1]["saida"]):
            log.warning("versões sem critério de escolha, ficam todas: %s", [r["saida"] for r in g])
            continue
        for r in g[1:]:
            _substituir(r, g[0], VERSAO_CORRIGIDA)
    return sum(r["status"] == SUBSTITUIDA for r in registros)


def _resumo_cleansed(registros: List[dict]) -> Dict[str, object]:
    tratados = [r for r in registros if r["status"] == TRATADO]
    motivos: Dict[str, int] = {}
    alertas: Dict[str, int] = {}
    regras: Dict[str, int] = {}
    for r in tratados:
        for k, v in r["descartes_por_motivo"].items():
            motivos[k] = motivos.get(k, 0) + v
        for k, v in r["padronizacoes"].items():
            regras[k] = regras.get(k, 0) + v
    for r in registros:
        for a in r["alertas_data"]:
            alertas[a] = alertas.get(a, 0) + 1
    return {
        "saidas": len(tratados),
        "familias": len({r["familia"] for r in tratados}),
        "linhas_reais": sum(r["linhas_reais"] for r in tratados),
        "descartes": motivos,
        "padronizacoes": dict(sorted(regras.items())),
        "versoes_substituidas": sum(r["status"] == SUBSTITUIDA for r in registros),
        "com_competencia": sum(r["competencia"] is not None for r in registros),
        "com_data_corte": sum(r["data_corte"] is not None for r in registros),
        "alertas_data": dict(sorted(alertas.items())),
    }


def etapa_cleansed(lake: Lake, tmp: Path) -> Dict[str, object]:
    """Tudo ou nada: só publica se todos os arquivos passaram pela conferência."""
    lake.baixar(LOG_ANALYTICS, tmp / "analytics_log.parquet")
    analytics = pq.read_table(tmp / "analytics_log.parquet").to_pylist()
    selecionados = [s for s in analytics if s["status"] == SELECIONADO]
    mapa = ler_mapa(ler_csv(lake, MAPA_CABECALHOS))
    familias = ler_familias(ler_csv(lake, MAPA_FAMILIAS))
    registros = planejar(selecionados, mapa, familias)
    gerado_em = datetime.now(timezone.utc).isoformat()
    for r in registros:
        r["gerado_em"] = gerado_em
    descartes, padronizacoes = _tratar_todos(lake, registros, tmp)
    datar(registros)
    escolher_versoes(registros)
    publicadas = set()
    for r in registros:
        if r["status"] in (TRATADO, SUBSTITUIDA):
            lake.subir(tmp / "saida" / r["familia"] / f"{r['saida']}.parquet", r["cleansed_key"])
            publicadas.add(r["cleansed_key"])
    # o controle fica; a saída que não é desta execução sai
    sobra = [
        k for k, _, _ in lake.listar(CLEANSED)
        if k.endswith(".parquet") and k not in publicadas and k not in CONTROLE_CLEANSED
    ]  # fmt: skip
    lake.apagar(sobra)
    escrever_log_cleansed(registros, tmp / "log.parquet")
    lake.subir(tmp / "log.parquet", LOG_CLEANSED)
    escrever_descartes(descartes, tmp / "descartes.parquet")
    lake.subir(tmp / "descartes.parquet", DESCARTES_CLEANSED)
    tabela = pa.Table.from_pylist(padronizacoes, schema=SCHEMA_PADRONIZACOES)
    pq.write_table(tabela, tmp / "padronizacoes.parquet")
    lake.subir(tmp / "padronizacoes.parquet", PADRONIZACOES)
    resumo = _resumo_cleansed(registros)
    log.info("cleansed: %s (%d removido(s) da execução anterior)", resumo, len(sobra))
    return resumo


# ---------------------------------------------------------------------------------------
# 4. colunas: evidência para o grafo de nomes (como cada dado muda de nome no tempo)
# ---------------------------------------------------------------------------------------

AMOSTRA_VALORES = 5_000  # valores distintos por coluna; basta para a sobreposição
NOTA_PROVAVEL = 0.6
PESOS = {"valores": 0.5, "nome": 0.3, "formato": 0.2}
# prefixos de tipo que mudam entre layouts sem mudar o dado
_PREFIXOS_TIPO = re.compile(
    r"^(cod|codigo|nu|num|numero|no|nom|nome|icodigo|id|txt|dsc|vlr|valor|qtd|qt|quantidade|"
    r"dt|dte|dat|data|bln)_+"
)
# o bext cola o tipo no nome (`vnome_municipio`, `csigla_uf`, `icodigo_empreendimento`)
_PREFIXO_COLADO = re.compile(
    r"^[vcidnb](?=(nome|sigla|codigo|ano|mes|faixa|instituicao|programa|valor|qtd|data|porte|"
    r"municipio|uf|situacao|descricao|percentual|numero|tipo|origem|empreendimento)(_|$))"
)
# conceito canônico (glossário: dbt/mcid/models/docs/glossario-mcid.md) -> padrão do nome
CONCEITOS = {
    "apf": r"^(cod_?|nu_?|numero_?|icodigo_?|id_?)?apf$",
    "codigo_empreendimento": r"^(i?cod(igo)?_?(do_)?|nu_)empreendimento(_bb)?$",
    "nome_empreendimento": r"^(v?nome_?(do_)?|txt_|dsc_|no_)?empreendimento$|^nomeempreendimento$",
    "uf": r"(^|_)(c?sigla_)?uf$",
    "municipio": r"^(v?nome_?(do_)?)?municipio$|^nomemunicipio$",
    "codigo_ibge_municipio": r"ibge|^codmunic",
    "quantidade_uh": r"^(qtd_|qt_|quantidade_(de_)?)?(uh|uhs|unidades)$",
    "faixa": r"^c?faixa(_de_renda)?$",
    "dt_contratacao": r"^(dat|data|dt|dte)_?(da_|de_)?contratacao(_bb)?$|^dte_assin_contrat",
    "cnpj": r"(^|_)cnpj($|_)",
}
_CONCEITOS_RX = {k: re.compile(v) for k, v in CONCEITOS.items()}


def conceito(coluna: str) -> Optional[str]:
    achados = [k for k, rx in _CONCEITOS_RX.items() if rx.search(coluna)]
    return achados[0] if len(achados) == 1 else None


def radical(coluna: str) -> str:
    """O nome sem os prefixos de tipo (`cod_`, `nu_`, `icodigo_`, `txt_`...)."""
    anterior = None
    while anterior != coluna:
        anterior, coluna = coluna, _PREFIXOS_TIPO.sub("", _PREFIXO_COLADO.sub("", coluna))
    return coluna


def _normalizar_valor(v: str) -> str:
    v = v.strip()
    return v[:-2] if re.fullmatch(r"-?\d+\.0", v) else v


def perfil(serie: pd.Series) -> dict:
    """Valores distintos (amostra, normalizados) e o formato da coluna."""
    valores = serie.dropna().astype(str).map(_normalizar_valor)
    valores = valores[valores != ""]
    if valores.empty:
        return {"valores": set(), "digitos": 0.0, "data": 0.0, "decimal": 0.0, "tamanho": 0.0}
    amostra = pd.Series(pd.unique(valores)[:AMOSTRA_VALORES], dtype="string")
    return {
        "valores": set(amostra),
        "digitos": float(amostra.str.fullmatch(r"\d+").mean()),
        "data": float(amostra.str.match(r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}").mean()),
        "decimal": float(amostra.str.fullmatch(r"-?\d+[.,]\d+").mean()),
        "tamanho": float(amostra.str.len().mean()),
    }


def nota_par(antes: str, depois: str, pa_: dict, pb: dict) -> dict:
    """Notas de 0 a 1 de que `antes` e `depois` são o mesmo dado com outro nome."""
    uniao = pa_["valores"] | pb["valores"]
    valores = len(pa_["valores"] & pb["valores"]) / len(uniao) if uniao else 0.0
    nome = SequenceMatcher(None, radical(antes), radical(depois)).ratio()
    diffs = [abs(pa_[k] - pb[k]) for k in ("digitos", "data", "decimal")]
    maior = max(pa_["tamanho"], pb["tamanho"]) or 1.0
    formato = 1 - (sum(diffs) + abs(pa_["tamanho"] - pb["tamanho"]) / maior) / 4
    total = PESOS["valores"] * valores + PESOS["nome"] * nome + PESOS["formato"] * formato
    return {
        "nota": round(total, 3),
        "nota_valores": round(valores, 3),
        "nota_nome": round(nome, 3),
        "nota_formato": round(formato, 3),
    }


def evolucao_colunas(log_cleansed: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por família x coluna: período, arquivos e layouts em que aparece."""
    t = log_cleansed[log_cleansed.status == TRATADO].copy()
    t["layout"] = t.colunas.map(lambda c: hashlib.md5("|".join(sorted(c)).encode()).hexdigest()[:8])
    linhas = t.explode("colunas").rename(columns={"colunas": "coluna"})
    return (
        linhas.groupby(["familia", "coluna"])
        .agg(
            primeira_competencia=("competencia", "min"),
            ultima_competencia=("competencia", "max"),
            n_arquivos=("saida", "nunique"),
            layouts=("layout", lambda s: sorted(set(s))),
        )
        .reset_index()
    )


def trocas_de_layout(log_cleansed: pd.DataFrame) -> List[dict]:
    """Em cada família, na ordem da competência, cada par de arquivos seguidos com colunas
    diferentes: o que saiu e o que entrou."""
    t = log_cleansed[log_cleansed.status == TRATADO].sort_values(
        ["familia", "competencia", "saida"]
    )
    trocas = []
    for familia, g in t.groupby("familia"):
        anterior = None
        for r in g.itertuples():
            if anterior is not None:
                sai = set(anterior.colunas) - set(r.colunas)
                entra = set(r.colunas) - set(anterior.colunas)
                if sai and entra:
                    trocas.append(
                        {
                            "familia": familia,
                            "competencia_troca": r.competencia,
                            "antes": anterior,
                            "depois": r,
                            "sai": sorted(sai),
                            "entra": sorted(entra),
                        }
                    )
            anterior = r
    return trocas


def candidatos_renomeacao(trocas: List[dict], ler_coluna) -> pd.DataFrame:
    """Pares (coluna que saiu, coluna que entrou) com as notas. `ler_coluna(registro,
    coluna)` devolve a série da coluna no arquivo do registro."""
    linhas, perfis = [], {}

    def _perfil(reg, coluna):
        chave = (reg.saida, coluna)
        if chave not in perfis:
            perfis[chave] = perfil(ler_coluna(reg, coluna))
        return perfis[chave]

    for troca in trocas:
        for a in troca["sai"]:
            for b in troca["entra"]:
                notas = nota_par(a, b, _perfil(troca["antes"], a), _perfil(troca["depois"], b))
                linhas.append(
                    {
                        "familia": troca["familia"],
                        "coluna_antes": a,
                        "coluna_depois": b,
                        "competencia_troca": troca["competencia_troca"],
                        "saida_antes": troca["antes"].saida,
                        "saida_depois": troca["depois"].saida,
                        **notas,
                    }
                )
    df = pd.DataFrame(linhas)
    if df.empty:
        return df
    df = df.sort_values("nota", ascending=False).drop_duplicates(
        ["familia", "coluna_antes", "coluna_depois"]
    )
    # provável: o melhor par dos dois lados e acima da nota mínima
    melhor_a = df.groupby(["familia", "coluna_antes"]).nota.transform("max") == df.nota
    melhor_b = df.groupby(["familia", "coluna_depois"]).nota.transform("max") == df.nota
    df["provavel"] = melhor_a & melhor_b & (df.nota >= NOTA_PROVAVEL)
    return df.reset_index(drop=True)


def _grupos(
    evolucao: pd.DataFrame, candidatos: pd.DataFrame
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """(família, coluna) -> (raiz do grupo de renomeação, família): união dos pares prováveis."""
    pai = {(r.familia, r.coluna): (r.familia, r.coluna) for r in evolucao.itertuples()}

    def raiz(x):
        while pai[x] != x:
            pai[x] = pai[pai[x]]
            x = pai[x]
        return x

    if not candidatos.empty:
        for r in candidatos[candidatos.provavel].itertuples():
            a, b = raiz((r.familia, r.coluna_antes)), raiz((r.familia, r.coluna_depois))
            if a != b:
                pai[a] = b
    return {k: raiz(k) for k in pai}


def conceitos_colunas(evolucao: pd.DataFrame, candidatos: pd.DataFrame) -> pd.DataFrame:
    """Proposta de nome canônico por família x coluna: o conceito do glossário quando o
    grupo de renomeação tem um; senão, o nome mais recente do grupo."""
    grupo = _grupos(evolucao, candidatos)
    ev = evolucao.copy()
    ev["grupo"] = [grupo[(r.familia, r.coluna)] for r in ev.itertuples()]
    ev["conceito_nome"] = ev.coluna.map(conceito)
    saida = []
    for _, g in ev.groupby("grupo"):
        conceitos_g = sorted(set(g.conceito_nome.dropna()))
        recente = g.sort_values(["ultima_competencia", "primeira_competencia"]).coluna.iloc[-1]
        if len(conceitos_g) == 1:
            canonico, origem = conceitos_g[0], "glossario"
        else:
            canonico, origem = recente, "renomeacao" if len(g) > 1 else "proprio"
        for r in g.itertuples():
            saida.append(
                {
                    "familia": r.familia,
                    "coluna": r.coluna,
                    "nome_canonico": canonico,
                    "origem": origem if len(conceitos_g) <= 1 else "conflito_glossario",
                    "nomes_no_grupo": sorted(set(g.coluna)),
                    "primeira_competencia": r.primeira_competencia,
                    "ultima_competencia": r.ultima_competencia,
                    "n_arquivos": r.n_arquivos,
                }
            )
    return pd.DataFrame(saida).sort_values(["nome_canonico", "familia", "primeira_competencia"])


def _id_mermaid(texto: str) -> str:
    return "n" + hashlib.md5(texto.encode()).hexdigest()[:10]


def grafo_mermaid(conceitos: pd.DataFrame) -> str:
    """Um grafo por nome canônico com mais de um nome de origem: cada nó é um nome numa
    família, com o período; as setas vão do nome mais antigo para o canônico."""
    partes = [
        "# Grafo de nomes de coluna dos dados históricos",
        "",
        "Gerado pela task `mapear_colunas` da `dados_historicos_dag`. Cada grafo é um nome "
        "canônico proposto; os nós são os nomes de origem, por família, com o período em que "
        "aparecem. Proposta para revisão: só vale depois de confirmada no de-para.",
        "",
    ]
    for canonico, g in conceitos.groupby("nome_canonico"):
        if g.coluna.nunique() < 2 and g.familia.nunique() < 2:
            continue
        partes += [f"## `{canonico}`", "", "```mermaid", "flowchart LR"]
        alvo = _id_mermaid(f"canonico:{canonico}")
        partes.append(f'    {alvo}(["{canonico}"])')
        for r in g.sort_values("primeira_competencia").itertuples():
            no = _id_mermaid(f"{r.familia}:{r.coluna}")
            rotulo = (
                f"{r.coluna}<br/>{r.familia}<br/>{r.primeira_competencia} a {r.ultima_competencia}"
            )
            partes.append(f'    {no}["{rotulo}"] --> {alvo}')
        partes += ["```", ""]
    return "\n".join(partes)


def etapa_colunas(lake: Lake, tmp: Path) -> Dict[str, int]:
    lake.baixar(LOG_CLEANSED, tmp / "log.parquet")
    log_cleansed = pd.read_parquet(tmp / "log.parquet")
    evolucao = evolucao_colunas(log_cleansed)
    trocas = trocas_de_layout(log_cleansed)
    baixados: Dict[str, Path] = {}

    def ler_coluna(reg, coluna) -> pd.Series:
        if reg.saida not in baixados:
            baixados[reg.saida] = lake.baixar(
                reg.cleansed_key, tmp / "arquivos" / f"{reg.saida}.parquet"
            )
        return pq.read_table(baixados[reg.saida], columns=[coluna]).column(0).to_pandas()

    candidatos = candidatos_renomeacao(trocas, ler_coluna)
    conceitos_df = conceitos_colunas(evolucao, candidatos)
    for nome, df, key in (
        ("evolucao", evolucao, EVOLUCAO),
        ("candidatos", candidatos, CANDIDATOS),
        ("conceitos", conceitos_df, CONCEITOS_ARQ),
    ):
        df.to_parquet(tmp / f"{nome}.parquet", index=False)
        lake.subir(tmp / f"{nome}.parquet", key)
    (tmp / "grafo.md").write_text(grafo_mermaid(conceitos_df), encoding="utf-8")
    lake.subir(tmp / "grafo.md", GRAFO)
    resumo = {
        "familias": int(evolucao.familia.nunique()),
        "colunas": len(evolucao),
        "trocas_de_layout": len(trocas),
        "pares_provaveis": int(candidatos.provavel.sum()) if len(candidatos) else 0,
        "nomes_canonicos": int(conceitos_df.nome_canonico.nunique()),
    }
    log.info("colunas: %s", resumo)
    return resumo


# ---------------------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------------------


@dag(
    dag_id="dados_historicos_dag",
    schedule=[ASSET_RAW],
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"forcar": False, "filtro": ""},
    default_args={"owner": "Lucas", "retries": 0, "execution_timeout": timedelta(hours=3)},
    tags=["dados_historicos", "mcmv", "lake"],
    doc_md=__doc__,
)
def dados_historicos_dag() -> None:
    @task
    def conferir_raw() -> dict:
        """Inventário da raw contra o manifesto da staging."""
        return conferir_raw_contra_manifesto(Lake())

    @task(outlets=[ASSET_STAGING])
    def raw_para_staging() -> dict:
        params = get_current_context()["params"]
        forcar, filtro = bool(params.get("forcar", False)), str(params.get("filtro") or "")
        with tempfile.TemporaryDirectory(prefix="historico-staging-") as tmp:
            return etapa_staging(Lake(), Path(tmp), forcar, filtro)

    @task(outlets=[ASSET_ANALYTICS])
    def staging_para_analytics() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-analytics-") as tmp:
            return etapa_analytics(Lake(), Path(tmp))

    @task(outlets=[ASSET_CLEANSED])
    def analytics_para_cleansed() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-cleansed-") as tmp:
            return etapa_cleansed(Lake(), Path(tmp))

    @task
    def mapear_colunas() -> dict:
        with tempfile.TemporaryDirectory(prefix="historico-colunas-") as tmp:
            return etapa_colunas(Lake(), Path(tmp))

    (
        conferir_raw()
        >> raw_para_staging()
        >> staging_para_analytics()
        >> analytics_para_cleansed()
        >> mapear_colunas()
    )


dag_instance = dados_historicos_dag()
