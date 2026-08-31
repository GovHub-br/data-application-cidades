# scripts/staging_para_bronze.py

"""
Carga Staging (Parquet no MinIO) -> Bronze (tabelas no Postgres), via pg_duckdb.

Terceira etapa do pipeline do data lake, depois de raw/ -> staging/. Para cada parquet
declarado numa família, executa um DROP + CREATE TABLE AS ... FROM read_parquet('s3://...')
direto no Postgres: é a extensão `pg_duckdb` (motor DuckDB embarcado) que lê o objeto do
MinIO usando o secret S3 do servidor. Os dados não passam pelo processo Python;
este script só decide O QUE carregar e dispara o SQL via psycopg2.

Diferente do raw_para_staging.py, este NÃO é um script generalista: ele não varre a
staging/ inteira. Só um pequeno subconjunto da staging vira tabela (o que alimenta modelos
dbt), então a carga é dirigida por família, declarada em scripts/bronze_familias.yml, e
`--familia` é obrigatório.

Decisões do projeto:
  - Todas as colunas como TEXT — mantém a decisão da staging (tudo string). A tipagem
    (datas/números) fica para a silver do dbt. A carga nunca falha por inferência errada.
  - Nomes de coluna normalizados para snake_case ASCII (normalizar_colunas) e truncados em
    63 bytes. Os parquets vindos do raw_para_staging.py já chegam normalizados — aqui é
    garantia e defesa para parquets de outros caminhos (ex.: DAGs de ingestão),
    além de dar uma base estável para comparar schemas entre cargas incrementais.
  - Full refresh por objeto: DROP + CREATE TABLE AS numa única transação Postgres. Não tem
    como duplicar linha, leitores enxergam a tabela antiga até o commit, e se falhar o
    rollback deixa a antiga de pé. É a saída simples no lugar de um upsert de verdade, que
    exigiria catálogo/chave declarada.
  - Colunas de linhagem (`_source_file`, `_ingested_at`, `_source_hash`) são preservadas.
  - Metadados (nº de linhas, colunas, hash) vêm do footer do parquet via pyarrow, sem
    baixar o arquivo — decide idempotência e dry-run sem tocar no Postgres.

Pré-requisito (fora do escopo deste script, feito uma vez na VM): pg_duckdb instalado e
ativo, e um secret S3 (`duckdb.create_simple_secret`) criado para o usuário de
DB_DW_USER_MCID. Sem isso, read_parquet falha com erro de credenciais.

IMPORTANTE (ordem no pipeline): rode DEPOIS do `raw_para_staging.py --apply`, que popula a
staging/.

Idempotência: tabela de controle lake._bronze_log com UNIQUE(familia, staging_key,
source_hash); objetos já carregados (mesmo hash) são pulados. Use --force para recarregar.
"""

import argparse
import fnmatch
import hashlib
import io
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import pyarrow.parquet as pq
import yaml
from dotenv import load_dotenv

from lake_utils import normalizar_colunas

# plugins/ (ClienteMinio) está na PYTHONPATH dentro do container Airflow; rodando
# standalone, adiciona plugins/ ao sys.path para o import resolver.
_plugins = Path(__file__).resolve().parents[1] / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))

from cliente_minio import ClienteMinio  # noqa: E402

load_dotenv()

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

PG_HOST = os.environ["DB_DW_HOST_MCID"]
PG_PORT = int(os.environ.get("DB_DW_PORT_MCID", 5432))
PG_USER = os.environ["DB_DW_USER_MCID"]
PG_PASSWORD = os.environ["DB_DW_PASSWORD_MCID"]
PG_DBNAME = os.environ["DB_DW_DBNAME_MCID"]

LAKE_SCHEMA = os.environ.get("LAKE_SCHEMA", "lake")
CONTROL_TABLE = "_bronze_log"

FAMILIAS_YML = Path(__file__).parent / "bronze_familias.yml"
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/")
AUDIT_PREFIX = "audit/bronze/"

# Postgres trunca identificadores em 63 bytes; truncar aqui (com sufixo determinístico)
# evita colisão silenciosa entre nomes longos que compartilham o mesmo prefixo.
PG_MAX_IDENT = 63

# Artefatos locais (arquivo de log, cópia local da auditoria) — úteis rodando standalone,
# mas o diretório do script pode não ser gravável (ex.: bind-mount no Airflow). Controlado
# por LAKE_LOCAL_ARTIFACTS (default "1"). O log em stderr fica sempre ativo.
_LOCAL_ARTIFACTS = os.environ.get("LAKE_LOCAL_ARTIFACTS", "1").lower() not in (
    "0",
    "false",
    "no",
)
_LOG_FILE = (
    Path(__file__).parent
    / f"staging_para_bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
if _LOCAL_ARTIFACTS:
    # Standalone: o script gerencia os próprios handlers (stderr + arquivo de log).
    logging.root.setLevel(logging.INFO)
    for _h in (
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ):
        _h.setFormatter(_formatter)
        logging.root.addHandler(_h)
# Sob o Airflow (_LOCAL_ARTIFACTS=0) NÃO adicionamos handlers ao root: o logger propaga
# para os handlers do Airflow. Um StreamHandler(sys.stderr) aqui criaria loop infinito.


# Famílias
def _carregar_familias() -> Dict[str, dict]:
    """Lê bronze_familias.yml e valida a forma de cada família."""
    if not FAMILIAS_YML.is_file():
        raise FileNotFoundError(f"Registro de famílias não encontrado: {FAMILIAS_YML}")
    with open(FAMILIAS_YML, encoding="utf-8") as fh:
        dados = yaml.safe_load(fh) or {}
    if not isinstance(dados, dict):
        raise ValueError(f"{FAMILIAS_YML.name}: esperado um mapa de famílias no topo.")

    for nome, cfg in dados.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Família '{nome}': esperado um mapa.")
        if not cfg.get("schema"):
            raise ValueError(f"Família '{nome}': campo 'schema' obrigatório.")
        objetos = cfg.get("objetos")
        if not objetos:
            raise ValueError(f"Família '{nome}': campo 'objetos' vazio ou ausente.")
        for i, obj in enumerate(objetos):
            _validar_objeto(nome, i, obj)
    return dados


def _validar_objeto(familia: str, i: int, obj: Any) -> None:
    """Valida uma entrada de `objetos`. Levanta ValueError com o caminho do erro."""
    onde = f"Família '{familia}', objeto {i}"
    if not isinstance(obj, dict):
        raise ValueError(f"{onde}: esperado um mapa.")
    if not obj.get("tabela"):
        raise ValueError(f"{onde}: falta 'tabela'.")
    if bool(obj.get("staging_key")) == bool(obj.get("padrao")):
        raise ValueError(
            f"{onde}: informe 'staging_key' OU 'padrao' (exatamente um dos dois)."
        )
    if not obj.get("data_regex"):
        return
    try:
        rx = re.compile(obj["data_regex"])
    except re.error as e:
        raise ValueError(f"{onde}: data_regex inválido — {e}") from e
    if rx.groups != 1:
        raise ValueError(
            f"{onde}: data_regex precisa de exatamente 1 grupo de captura "
            f"(tem {rx.groups})."
        )


# Data no nome do arquivo, quando o YAML não declara `data_regex`. Aceita 6 (aaaamm) a 8
# (aaaammdd) dígitos; o `_ultimo` da busca pega o grupo mais à direita.
_DATA_PADRAO = re.compile(r"(\d{6,8})")


def _chave_versao(key: str, data_regex: Optional[str]) -> Tuple[str, str]:
    """(data, key) para ordenar versões de um mesmo dado. Sem data, ordena só pela key.

    Ordenar pela key inteira não serve: as versões estão espalhadas em pastas diferentes
    (`Novo MCMV - FAR/` e `.../Arquivados/...`), e aí o caminho pesaria mais que
    a data. Por isso a data sai do NOME do arquivo.
    """
    nome = os.path.basename(key)
    if data_regex:
        m = re.search(data_regex, nome)
        return (m.group(1) if m else "", key)
    achados = _DATA_PADRAO.findall(nome)
    return (achados[-1] if achados else "", key)


def _resolver_objeto(obj: dict, keys_staging: List[str]) -> Optional[str]:
    """Key de staging a carregar: a declarada, ou a versão mais recente do padrão.

    Retorna None quando o padrão não casa com nada — o chamador transforma isso em erro
    registrado, para a família não falhar inteira por causa de um objeto ausente.
    """
    if obj.get("staging_key"):
        return str(obj["staging_key"])
    candidatos = [k for k in keys_staging if fnmatch.fnmatch(k, obj["padrao"])]
    if not candidatos:
        return None
    return max(candidatos, key=lambda k: _chave_versao(k, obj.get("data_regex")))


def _listar_familias() -> None:
    familias = _carregar_familias()
    print(f"Famílias declaradas em {FAMILIAS_YML.name}:\n")
    for nome, cfg in sorted(familias.items()):
        descricao = " ".join((cfg.get("descricao") or "").split())
        print(f"  {nome}")
        print(f"    schema : {cfg['schema']}")
        if descricao:
            print(f"    sobre  : {descricao}")
        print(f"    objetos: {len(cfg['objetos'])}")
        for obj in cfg["objetos"]:
            origem = obj.get("staging_key") or f"{obj['padrao']}  (mais recente)"
            print(f"      - {obj['tabela']}  <-  {origem}")
        print()


# Infra: conexões / controle
def _conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def _criar_control_table(conn_str: str) -> None:
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE_SCHEMA};")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LAKE_SCHEMA}.{CONTROL_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    execution_id  TEXT,
                    familia       TEXT NOT NULL,
                    staging_key   TEXT NOT NULL,
                    target_table  TEXT,
                    source_file   TEXT,
                    source_hash   TEXT,
                    staging_etag  TEXT,
                    n_linhas      BIGINT,
                    n_colunas     INT,
                    colunas_novas JSONB,
                    colunas_sumidas JSONB,
                    status        TEXT,
                    error_message TEXT,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (familia, staging_key, source_hash)
                );
                """
            )
            # Auto-migração: staging_etag entrou depois da primeira versão da tabela.
            cur.execute(
                f"""
                ALTER TABLE {LAKE_SCHEMA}.{CONTROL_TABLE}
                ADD COLUMN IF NOT EXISTS staging_etag TEXT;
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_bronze_log_status
                ON {LAKE_SCHEMA}.{CONTROL_TABLE} (status);
                """
            )
            conn.commit()
    log.info("Tabela de controle %s.%s garantida.", LAKE_SCHEMA, CONTROL_TABLE)


def _carregar_carregados(conn_str: str, familia: str) -> set:
    """(staging_key, source_hash, staging_etag) já materializados ('loaded') na família.

    O `staging_etag` precisa entrar na chave: `source_hash` é o hash do arquivo em raw/, e
    o mesmo raw pode gerar um parquet DIFERENTE se o raw_para_staging.py mudar (foi o que
    aconteceu na correção do detectar_encoding — o raw era o mesmo, mas o parquet saiu com
    o texto certo em vez de mojibake). Só com o hash da origem, a bronze pularia a recarga
    e continuaria servindo o parquet antigo silenciosamente.

    Só o `--apply` (status 'loaded') conta para idempotência; dry-runs não bloqueiam.
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT staging_key, source_hash, staging_etag
                FROM {LAKE_SCHEMA}.{CONTROL_TABLE}
                WHERE familia = %s AND status = 'loaded' AND source_hash IS NOT NULL
                """,
                (familia,),
            )
            return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def _registrar_control(conn_str: str, rec: dict) -> None:
    from psycopg2.extras import Json

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {LAKE_SCHEMA}.{CONTROL_TABLE}
                    (execution_id, familia, staging_key, target_table, source_file,
                     source_hash, staging_etag, n_linhas, n_colunas, colunas_novas,
                     colunas_sumidas, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (familia, staging_key, source_hash) DO UPDATE SET
                    execution_id    = EXCLUDED.execution_id,
                    target_table    = EXCLUDED.target_table,
                    source_file     = EXCLUDED.source_file,
                    staging_etag    = EXCLUDED.staging_etag,
                    n_linhas        = EXCLUDED.n_linhas,
                    n_colunas       = EXCLUDED.n_colunas,
                    colunas_novas   = EXCLUDED.colunas_novas,
                    colunas_sumidas = EXCLUDED.colunas_sumidas,
                    status          = EXCLUDED.status,
                    error_message   = EXCLUDED.error_message,
                    created_at      = NOW()
                """,
                (
                    rec["execution_id"],
                    rec["familia"],
                    rec["staging_key"],
                    rec["target_table"],
                    rec["source_file"],
                    rec["source_hash"],
                    rec["staging_etag"],
                    rec["n_linhas"],
                    rec["n_colunas"],
                    Json(rec["colunas_novas"]),
                    Json(rec["colunas_sumidas"]),
                    rec["status"],
                    rec["error_message"],
                ),
            )
            conn.commit()


# Identificadores Postgres
def _truncar_ident(nome: str) -> str:
    """Trunca em 63 bytes preservando unicidade via hash do nome completo."""
    if len(nome.encode("utf-8")) <= PG_MAX_IDENT:
        return nome
    h = hashlib.md5(nome.encode("utf-8")).hexdigest()[:6]
    return f"{nome[: PG_MAX_IDENT - 7]}_{h}"


def _colunas_postgres(nomes: List[str]) -> Tuple[List[str], bool]:
    """Normaliza p/ snake_case, trunca em 63 bytes e deduplica.

    `normalizar_colunas` (lake_utils) é a mesma função usada no raw_para_staging.py, então
    um parquet que veio de lá passa incólume; o trabalho real acontece para parquets
    gerados por outros caminhos.

    Ressalva: `norm_header` faz `.strip("_")`, o que comeria o prefixo das colunas de
    linhagem (`_source_file`, `_ingested_at`, `_source_hash`). O underscore inicial é
    convenção da staging para separar linhagem de dado, então é reposto aqui — sem isso a
    bronze renomearia essas três colunas silenciosamente. Repor underscore nunca gera
    colisão nova (só torna o nome mais distinto), e a dedup abaixo cobre o resto.

    Retorna (finais, houve_mudanca).
    """
    normalizados, _mapa = normalizar_colunas(nomes)
    finais: List[str] = []
    usados: set = set()
    for original, nome in zip(nomes, normalizados):
        prefixo = "_" * (len(original) - len(original.lstrip("_")))
        if prefixo and not nome.startswith("_"):
            nome = prefixo + nome
        base = _truncar_ident(nome)
        final = base
        n = 2
        while final in usados:
            sufixo = f"_{n}"
            final = _truncar_ident(base[: PG_MAX_IDENT - len(sufixo)] + sufixo)
            n += 1
        usados.add(final)
        finais.append(final)
    return finais, finais != list(nomes)


# Leitura do parquet (só o footer, via HTTP Range)
def _fs_minio() -> Any:
    from pyarrow.fs import S3FileSystem

    endpoint = MINIO_ENDPOINT
    scheme = "http"
    if endpoint.startswith("http://"):
        endpoint = endpoint[len("http://") :]
    elif endpoint.startswith("https://"):
        endpoint, scheme = endpoint[len("https://") :], "https"
    return S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=endpoint,
        scheme=scheme,
    )


def _etag_staging(minio: ClienteMinio, staging_key: str) -> Optional[str]:
    """ETag do objeto de staging — identifica a VERSÃO do parquet, não a do raw.

    É o que separa "o arquivo de origem mudou" (source_hash) de "o parquet foi regerado"
    (etag). Sem isso, um conserto no raw_para_staging.py que reescreve o parquet a partir
    do mesmo raw passaria despercebido pela idempotência da bronze.
    """
    try:
        resp = minio.s3.head_object(Bucket=MINIO_BUCKET, Key=staging_key)
        etag = resp.get("ETag")
        return etag.strip('"') if etag else None
    except Exception as e:  # noqa: BLE001
        # ETag é reforço de idempotência, não requisito: sem ele a carga ainda funciona.
        log.warning("Não foi possível ler o ETag de %s: %s", staging_key, e)
        return None


def _ler_metadados(
    fs: Any, staging_key: str
) -> Tuple[int, List[str], Optional[str], Optional[str]]:
    """Retorna (n_linhas, colunas, source_hash, source_file) — só o footer do parquet."""
    pf = pq.ParquetFile(f"{MINIO_BUCKET}/{staging_key}", filesystem=fs)
    n_linhas = pf.metadata.num_rows
    colunas = list(pf.schema_arrow.names)
    meta = pf.schema_arrow.metadata or {}

    def _get(chave: bytes) -> Optional[str]:
        return meta[chave].decode("utf-8") if chave in meta else None

    return n_linhas, colunas, _get(b"source_hash"), _get(b"source_file")


# Drift de schema
def _colunas_existentes(conn_str: str, schema: str, tabela: str) -> Optional[List[str]]:
    """Colunas da tabela bronze atual, ou None se ela ainda não existe."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, tabela),
            )
            linhas = [r[0] for r in cur.fetchall()]
    return linhas or None


def _checar_drift(
    conn_str: str, schema: str, tabela: str, colunas_pg: List[str]
) -> Tuple[List[str], List[str]]:
    """Compara o schema do parquet com o da tabela já existente.

    A carga é full refresh, então o drift não bloqueia nada — mas precisa sair no log e
    na tabela de controle, senão uma coluna que some da origem vira uma quebra silenciosa
    lá na frente, no model dbt que a referencia.
    """
    atuais = _colunas_existentes(conn_str, schema, tabela)
    if atuais is None:
        return [], []
    novas = [c for c in colunas_pg if c not in set(atuais)]
    sumidas = [c for c in atuais if c not in set(colunas_pg)]
    if novas or sumidas:
        log.warning(
            "%s.%s — schema mudou desde a última carga: +%s / -%s",
            schema,
            tabela,
            novas or "nenhuma",
            sumidas or "nenhuma",
        )
    return novas, sumidas


# Carga via pg_duckdb (SQL executado no próprio Postgres)
_MEMORY_LIMIT_RE = re.compile(r"^\d+(KB|MB|GB|TB)?$", re.IGNORECASE)

# Poder (ou não) setar os GUCs do pg_duckdb é propriedade fixa da role, não algo que muda
# de objeto pra objeto: avisa uma vez por execução em vez de duas linhas por parquet.
_GUC_AVISADO: set = set()


def _avisar_guc(guc: str, erro: Exception) -> None:
    if guc not in _GUC_AVISADO:
        _GUC_AVISADO.add(guc)
        log.warning(
            "Sem permissão para ajustar %s (segue com o default do servidor): %s",
            guc,
            str(erro).strip(),
        )


def _ajustar_recursos_sessao(conn: Any, memory_limit: str, threads: int) -> None:
    """Aplica limites de recursos do pg_duckdb nesta sessão (best-effort).

    `duckdb.max_memory`/`duckdb.threads` são GUCs da extensão, válidos só pra conexão
    atual. Se a role não puder alterá-los, avisa e segue com o default do servidor. Cada
    SET roda no próprio commit/rollback: um SET que falha aborta a transação, e sem
    o rollback aqui o DROP/CREATE seguinte falharia junto.
    """
    if not _MEMORY_LIMIT_RE.match(memory_limit):
        log.warning("memory-limit %r em formato inesperado, ignorando.", memory_limit)
    else:
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET duckdb.max_memory = '{memory_limit}';")
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            _avisar_guc("duckdb.max_memory", e)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET duckdb.threads = {threads};")
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        _avisar_guc("duckdb.threads", e)


def _select_texto(colunas_parquet: List[str], colunas_pg: List[str]) -> str:
    """SELECT com todas as colunas explicitamente CAST para VARCHAR (-> TEXT no Postgres).

    pg_duckdb expõe o retorno de read_parquet() como um único registro opaco pro parser do
    Postgres: as colunas não podem ser referenciadas como "coluna" direto, tem que ser via
    r['coluna'] (alias da função + acesso por chave) — sem isso dá "column ... does not
    exist" com um HINT nesse sentido.
    """
    partes = [
        f"""CAST(r['{orig.replace("'", "''")}'] AS VARCHAR) AS "{final}\""""
        for orig, final in zip(colunas_parquet, colunas_pg)
    ]
    return ", ".join(partes)


def _carregar_tabela(
    conn_str: str,
    staging_key: str,
    schema: str,
    tabela: str,
    colunas_parquet: List[str],
    colunas_pg: List[str],
    memory_limit: str,
    threads: int,
) -> int:
    """DROP + CREATE TABLE AS na mesma transação; retorna nº de linhas carregadas.

    O read_parquet roda dentro do Postgres via pg_duckdb, com o secret S3 do
    servidor — o parquet nunca passa pelo processo Python. Como DROP e CREATE estão na
    mesma transação, não existe janela em que a tabela some, nem como duplicar linha.
    """
    uri = f"s3://{MINIO_BUCKET}/{staging_key}"
    select = _select_texto(colunas_parquet, colunas_pg)
    conn = psycopg2.connect(conn_str)
    try:
        _ajustar_recursos_sessao(conn, memory_limit, threads)
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cur.execute(f'DROP TABLE IF EXISTS {schema}."{tabela}";')
            cur.execute(
                f'CREATE TABLE {schema}."{tabela}" AS '
                f"SELECT {select} FROM read_parquet(%s) AS r;",
                (uri,),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM {schema}."{tabela}";')
            linha = cur.fetchone()
            return int(linha[0]) if linha else 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Processamento de um objeto
def _novo_registro(execution_id: str, familia: str, staging_key: str) -> dict:
    return {
        "execution_id": execution_id,
        "familia": familia,
        "staging_key": staging_key,
        "target_table": None,
        "source_file": None,
        "source_hash": None,
        "staging_etag": None,
        "n_linhas": 0,
        "n_colunas": 0,
        "colunas_novas": [],
        "colunas_sumidas": [],
        "status": None,
        "error_message": None,
    }


def processar_objeto(
    conn_str: str,
    fs: Any,
    minio: ClienteMinio,
    familia: str,
    schema: str,
    obj: dict,
    execution_id: str,
    apply: bool,
    carregados: set,
    memory_limit: str,
    threads: int,
    keys_staging: List[str],
) -> dict:
    t0 = time.time()
    tabela = obj["tabela"]
    staging_key = _resolver_objeto(obj, keys_staging)
    rec = _novo_registro(execution_id, familia, staging_key or obj.get("padrao", "?"))
    rec["target_table"] = f"{schema}.{tabela}"
    if staging_key is None:
        rec["status"] = "error"
        rec["error_message"] = f"nenhum objeto casa com o padrão {obj['padrao']!r}"
        log.error("✗ %s — %s", tabela, rec["error_message"])
        rec["_segundos"] = round(time.time() - t0, 2)
        return rec
    if obj.get("padrao"):
        log.info(
            "%s — versão mais recente do padrão: %s", tabela, staging_key.split("/")[-1]
        )
    try:
        n_linhas, colunas_parquet, source_hash, source_file = _ler_metadados(
            fs, staging_key
        )
        rec["source_hash"] = source_hash
        rec["source_file"] = source_file
        rec["staging_etag"] = _etag_staging(minio, staging_key)
        rec["n_linhas"] = n_linhas
        rec["n_colunas"] = len(colunas_parquet)
        rec["target_table"] = f"{schema}.{tabela}"

        if not colunas_parquet:
            rec["status"] = "skipped_empty"
            return rec

        chave = (staging_key, source_hash, rec["staging_etag"])
        if source_hash is not None and chave in carregados:
            rec["status"] = "skipped_already"
            return rec

        colunas_pg, mudou = _colunas_postgres(colunas_parquet)
        if mudou:
            log.info(
                "%s — nomes de coluna normalizados/truncados p/ o Postgres", staging_key
            )

        novas, sumidas = _checar_drift(conn_str, schema, tabela, colunas_pg)
        rec["colunas_novas"] = novas
        rec["colunas_sumidas"] = sumidas

        if not apply:
            rec["status"] = "dry_run"
            return rec

        n_pg = _carregar_tabela(
            conn_str,
            staging_key,
            schema,
            tabela,
            colunas_parquet,
            colunas_pg,
            memory_limit,
            threads,
        )
        if n_pg != n_linhas:
            raise ValueError(
                f"nº de linhas divergem: postgres={n_pg} != parquet={n_linhas}"
            )
        rec["status"] = "loaded"
        return rec

    except Exception as e:  # noqa: BLE001
        rec["status"] = "error"
        rec["error_message"] = str(e)[:500]
        log.error("✗ %s: %s", staging_key, e)
        return rec
    finally:
        rec["_segundos"] = round(time.time() - t0, 2)


# Auditoria (parquet)
def _gravar_auditoria(
    minio: ClienteMinio, execution_id: str, registros: List[dict]
) -> None:
    df = pd.DataFrame(registros)
    for col in ("colunas_novas", "colunas_sumidas"):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: ", ".join(v) if v else "")
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    key = f"{AUDIT_PREFIX}execution_id={execution_id}/part-0.parquet"
    minio.put_object(key, buf.getvalue())
    if _LOCAL_ARTIFACTS:
        local = Path(__file__).parent / f"auditoria_bronze_{execution_id}.parquet"
        df.to_parquet(local, engine="pyarrow", index=False)
        log.info("Auditoria: s3://%s/%s (cópia local: %s)", MINIO_BUCKET, key, local)
    else:
        log.info("Auditoria: s3://%s/%s", MINIO_BUCKET, key)


# Execução
def run(
    familia: str,
    apply: bool = False,
    force: bool = False,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
) -> Dict[str, int]:
    """Materializa os parquets de uma família na bronze. Retorna a contagem por status.

    Ponto de entrada reutilizável (CLI via main(); uma DAG chama run(fam, apply=True)).
    """
    familias = _carregar_familias()
    if familia not in familias:
        disponiveis = ", ".join(sorted(familias)) or "(nenhuma)"
        raise KeyError(f"Família '{familia}' não declarada. Disponíveis: {disponiveis}")

    cfg = familias[familia]
    schema = cfg["schema"]
    objetos = cfg["objetos"]
    memory_limit = memory_limit or os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB")
    threads = threads if threads is not None else int(os.environ.get("DUCKDB_THREADS", 4))
    execution_id = uuid.uuid4().hex

    log.info("=" * 70)
    log.info(
        "Execução %s | família=%s | modo=%s | destino=%s",
        execution_id,
        familia,
        f"APPLY (Postgres {schema}.*)" if apply else "DRY-RUN (nada gravado)",
        schema,
    )
    log.info("=" * 70)

    conn_str = _conn_str()
    _criar_control_table(conn_str)
    carregados = set() if force else _carregar_carregados(conn_str, familia)

    minio = ClienteMinio()
    fs = _fs_minio()

    # Uma listagem só, reaproveitada por todos os padrões da família.
    keys_staging: List[str] = []
    if any(o.get("padrao") for o in objetos):
        keys_staging = [k for k, _ in minio.listar_objetos(STAGING_PREFIX)]
        log.info(
            "Objetos em %s: %d (para resolver os padrões).",
            STAGING_PREFIX,
            len(keys_staging),
        )

    registros: List[dict] = []
    contagem: Dict[str, int] = {}

    for i, obj in enumerate(objetos, start=1):
        rec = processar_objeto(
            conn_str,
            fs,
            minio,
            familia,
            schema,
            obj,
            execution_id,
            apply,
            carregados,
            memory_limit,
            threads,
            keys_staging,
        )
        registros.append(rec)
        contagem[rec["status"]] = contagem.get(rec["status"], 0) + 1
        if rec["source_hash"] is not None and rec["status"] != "skipped_already":
            _registrar_control(conn_str, rec)

        icone = {"loaded": "✓", "dry_run": "◐", "error": "✗"}.get(rec["status"], "·")
        log.info(
            "%s [%d/%d] %s — %s | tabela=%s | linhas=%s | cols=%d | %.1fs",
            icone,
            i,
            len(objetos),
            rec["staging_key"],  # já resolvido (a key declarada ou a versão do padrão)
            rec["status"],
            rec["target_table"],
            f"{rec['n_linhas']:,}".replace(",", "."),
            rec["n_colunas"],
            rec["_segundos"],
        )

    if registros:
        _gravar_auditoria(minio, execution_id, registros)

    log.info("=" * 70)
    log.info("Concluído. Família %s — objetos: %d", familia, len(objetos))
    for status, n in sorted(contagem.items()):
        log.info("  %-20s %d", status, n)
    if not apply:
        log.info("DRY-RUN — nenhuma tabela criada. Use --apply para gravar no Postgres.")
    if _LOCAL_ARTIFACTS:
        log.info("Log: %s", _LOG_FILE)
    return contagem


# Main
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carga Staging (Parquet/MinIO) -> Bronze (Postgres) via pg_duckdb.",
    )
    parser.add_argument(
        "--familia",
        help="Família a carregar (ver bronze_familias.yml). Use --listar para as opções.",
    )
    parser.add_argument(
        "--listar",
        action="store_true",
        help="Lista as famílias declaradas e sai.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Cria/substitui as tabelas no Postgres. Sem esta flag roda em dry-run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recarrega objetos já materializados (ignora a idempotência).",
    )
    parser.add_argument(
        "--memory-limit",
        default=os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB"),
        help="Limite de memória do pg_duckdb p/ esta sessão (default 4GB).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=int(os.environ.get("DUCKDB_THREADS", 4)),
        help="Threads do pg_duckdb p/ esta sessão (default 4).",
    )
    args = parser.parse_args()

    if args.listar:
        _listar_familias()
        return
    if not args.familia:
        parser.error("--familia é obrigatório (ou use --listar).")

    run(
        familia=args.familia,
        apply=args.apply,
        force=args.force,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
