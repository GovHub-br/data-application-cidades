#!/usr/bin/env python3
"""Publicacao do eixo historico no Postgres — MODO B da change
`pipeline-bronze-historica-destino-trocavel`.

Copia tabelas JA MATERIALIZADAS no arquivo DuckDB local para o Postgres `prod`,
uma familia por vez. NAO le a staging MinIO: a origem de cada tabela e o arquivo
local (D2). O motor DuckDB roda neste processo, FORA do Postgres (D1) — o banco
so recebe DDL e insercao em streaming pelo `postgres_scanner`, nunca
`pg_duckdb`.

Cada carga:
  1. copia  `CREATE TABLE <destino> AS SELECT * FROM <tabela local>`;
  2. confere `count(*)` contra a tabela local e mede `pg_total_relation_size`;
  3. registra uma linha em `lake._bronze_historico_log`.

Divergencia de contagem interrompe a publicacao: a linha do log recebe
`status = 'error'` e as familias seguintes NAO sao carregadas.

Escritas permitidas no `prod` (D7): CREATE SCHEMA IF NOT EXISTS, CREATE TABLE,
DROP TABLE das tabelas do eixo e insercao. Nenhum ALTER SYSTEM / ALTER ROLE /
ALTER DATABASE / CREATE EXTENSION.

Uso:
    python3 scripts/publicar_historico.py --listar
    python3 scripts/publicar_historico.py --tabela dados_historicos.bronze_..._entrada_bb
    python3 scripts/publicar_historico.py --grupo bronzes --dry-run
    python3 scripts/publicar_historico.py --grupo tudo
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb
import psycopg2

# --- inventario de tabelas publicaveis -------------------------------------
# `familia` casa com o mapa de macros/historico/familias.sql quando a tabela e
# uma bronze de serie; para silver/gold e o nome do dominio.

BRONZES_SERIE = [
    # (schema, tabela, familia, glob de origem)
    ("dados_historicos", "bronze_mcmv_historico_serie_entrada_bb",
     "entrada_bb", "dados_historicos/*entrada_bb*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_snh_bb",
     "snh_bb", "dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_bb*.parquet"),
    ("dados_historicos", "bronze_reloginho_snh_entregas_evento_bb",
     "entregas_bb", "dados_historicos/*_da_entrega_da_unidade_af_bb.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_int054",
     "INT054", "sftp/fabrica/GEFUS/**/INT054_*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_int059",
     "INT059", "sftp/fabrica/GEFUS/**/INT059_*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_int057",
     "INT057", "sftp/fabrica/GEFUS/**/INT057_*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_int040",
     "INT040", "sftp/fabrica/GEFUS/**/INT040_*.parquet"),
    ("dados_historicos", "bronze_reloginho_snh_entregas_evento_caixa",
     "entregas_caixa", "dados_historicos/*_af_caixa_entregas.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_snh_caixa",
     "snh_caixa", "dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_caixa*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_empreendimento_int065",
     "INT065", "sftp/fabrica/GEFUS/**/INT065_*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_serie_bases_relatorio_executivo",
     "bases_relatorio_executivo", "dados_historicos/*bases_relat*rio_executivo*.parquet"),
    ("dados_historicos", "bronze_mcmv_historico_serie_min_cidades",
     "min_cidades", "dados_historicos/*min_cidades*.parquet"),
    # bext e a maior transacao isolada (5,66 M linhas) — ultima das bronzes (D8).
    ("dados_historicos", "bronze_mcmv_historico_serie_bext",
     "bext", "dados_historicos/*bext*.parquet"),
]

SILVERS_GOLDS = [
    ("conjuntura", "silver_mcmv_historico_serie_anual_ogu_fgts", "mcmv_historico"),
    ("reloginho", "silver_historico_snh_entregas_mes", "reloginho"),
    ("reloginho", "silver_historico_snh_apf_mes", "reloginho"),
    ("reloginho", "gold_indicadores_reloginho", "reloginho"),
    ("reloginho", "gold_indicadores_reloginho_frente", "reloginho"),
    ("reloginho", "gold_indicadores_reloginho_entregas", "reloginho"),
    ("reloginho", "gold_resumo_reloginho_dashboard", "reloginho"),
    ("reloginho", "gold_indicadores_gargalo_desempenho", "reloginho"),
    ("reloginho", "gold_resumo_gargalo_desempenho_dashboard", "reloginho"),
    ("empreendimentos_fds", "silver_historico_empreendimento", "mcmv_historico"),
    ("empreendimento_far", "silver_historico_empreendimento", "mcmv_historico"),
    ("empreendimento_rural", "silver_historico_empreendimento", "mcmv_historico"),
    ("serie_historica", "gold_serie_mensal", "mcmv_historico"),
    ("serie_historica", "gold_snapshot_empreendimento_atual", "mcmv_historico"),
    # a silver da serie executiva e a maior (10,2 M linhas) — por ultimo.
    ("dados_historicos", "silver_mcmv_historico_serie_executiva", "mcmv_historico"),
]


@dataclass
class Alvo:
    schema: str
    tabela: str
    familia: str
    staging_key: str

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.tabela}"


def inventario(grupo: str) -> list[Alvo]:
    bronzes = [Alvo(s, t, f, g) for s, t, f, g in BRONZES_SERIE]
    outras = [Alvo(s, t, f, f"local:{s}.{t}") for s, t, f in SILVERS_GOLDS]
    return {"bronzes": bronzes, "silvers": outras, "tudo": bronzes + outras}[grupo]


LOG_DDL = """
create schema if not exists lake;

create table if not exists lake._bronze_historico_log (
    -- `serial` (integer), nao bigserial: o cenario de compatibilidade da D6
    -- exige que toda coluna de lake._bronze_log exista aqui com o MESMO tipo.
    id                serial primary key,
    execution_id      text,
    familia           text        not null,
    staging_key       text        not null,
    target_table      text,
    source_file       text,
    source_hash       text,
    staging_etag      text,
    n_linhas          bigint,
    n_colunas         integer,
    colunas_novas     jsonb,
    colunas_sumidas   jsonb,
    status            text,
    error_message     text,
    created_at        timestamptz default now(),
    -- alem do log dos colegas (lake._bronze_log), o que o caso multi-arquivo exige:
    n_arquivos        integer,
    dt_referencia_min date,
    dt_referencia_max date,
    modo              text,
    target            text,
    constraint _bronze_historico_log_familia_staging_key_source_hash_key
        unique (familia, staging_key, source_hash)
);
"""


def env(nome: str, default: str | None = None) -> str:
    v = os.environ.get(nome, default)
    if v is None:
        sys.exit(f"env var obrigatoria ausente: {nome}")
    return v


def pg_conn():
    return psycopg2.connect(
        host=env("DB_DW_HOST_MCID"),
        user=env("DB_DW_USER_MCID"),
        password=env("DB_DW_PASSWORD_MCID"),
        port=env("DB_DW_PORT_MCID", "5432"),
        dbname=env("DB_DW_DBNAME_MCID", "cidades"),
        connect_timeout=15,
    )


def pg_dsn() -> str:
    return (
        f"dbname={env('DB_DW_DBNAME_MCID', 'cidades')} host={env('DB_DW_HOST_MCID')} "
        f"port={env('DB_DW_PORT_MCID', '5432')} user={env('DB_DW_USER_MCID')} "
        f"password={env('DB_DW_PASSWORD_MCID')}"
    )


def garantir_log(cur) -> None:
    """Tarefa 6.1 — schema `lake` e a tabela de log, ambos idempotentes."""
    cur.execute(LOG_DDL)


def perfil_local(con, alvo: Alvo) -> dict:
    """Fingerprint da tabela local: contagem, colunas, arquivos de origem e
    janela de dt_referencia. Le SO o arquivo local — nenhuma requisicao ao
    MinIO (o modo B nao rele a staging)."""
    cols = [r[0] for r in con.execute(
        f'describe select * from "{alvo.schema}"."{alvo.tabela}"'
    ).fetchall()]
    n_linhas = con.execute(
        f'select count(*) from "{alvo.schema}"."{alvo.tabela}"'
    ).fetchone()[0]

    arquivos: list[str] = []
    if "source_file" in cols:
        arquivos = sorted(
            r[0] for r in con.execute(
                f'select distinct source_file from "{alvo.schema}"."{alvo.tabela}" '
                "where source_file is not null"
            ).fetchall()
        )

    dt_min = dt_max = None
    if "dt_referencia" in cols:
        dt_min, dt_max = con.execute(
            f'select min(dt_referencia), max(dt_referencia) '
            f'from "{alvo.schema}"."{alvo.tabela}"'
        ).fetchone()

    if arquivos:
        # identidade da carga = lista ordenada dos arquivos que casaram o glob.
        source_hash = hashlib.md5("\n".join(arquivos).encode()).hexdigest()
    else:
        # Tabelas derivadas (silver/gold) nao tem source_file: o fingerprint cai
        # para forma + volume. Basta para auditar a carga; NAO e garantia de
        # idempotencia como nas bronzes — por isso --pular-iguais so e confiavel
        # para as bronzes.
        source_hash = hashlib.md5(
            f"{n_linhas}|{len(cols)}|{alvo.fqn}".encode()
        ).hexdigest()

    return {
        "n_linhas": n_linhas,
        "n_colunas": len(cols),
        "colunas": cols,
        "arquivos": arquivos,
        "source_hash": source_hash,
        "dt_referencia_min": dt_min,
        "dt_referencia_max": dt_max,
    }


def colunas_no_destino(cur, alvo: Alvo) -> list[str] | None:
    cur.execute(
        "select column_name from information_schema.columns "
        "where table_schema = %s and table_name = %s order by ordinal_position",
        (alvo.schema, alvo.tabela),
    )
    linhas = [r[0] for r in cur.fetchall()]
    return linhas or None


def gravar_log(cur, **campos) -> None:
    campos.setdefault("created_at", datetime.now(timezone.utc))
    colunas = ", ".join(campos)
    valores = ", ".join(f"%({c})s" for c in campos)
    cur.execute(
        f"insert into lake._bronze_historico_log ({colunas}) values ({valores}) "
        "on conflict (familia, staging_key, source_hash) do update set "
        "  target_table = excluded.target_table, n_linhas = excluded.n_linhas, "
        "  n_colunas = excluded.n_colunas, status = excluded.status, "
        "  error_message = excluded.error_message, created_at = excluded.created_at, "
        "  n_arquivos = excluded.n_arquivos, modo = excluded.modo, target = excluded.target",
        campos,
    )


def ja_publicada(cur, alvo: Alvo, source_hash: str) -> bool:
    cur.execute(
        "select 1 from lake._bronze_historico_log "
        "where familia = %s and staging_key = %s and source_hash = %s "
        "and status = 'loaded'",
        (alvo.familia, alvo.staging_key, source_hash),
    )
    return cur.fetchone() is not None


def publicar(con, pg, alvo: Alvo, execution_id: str, dry_run: bool,
             pular_iguais: bool) -> bool:
    """Publica uma tabela. Devolve False quando a verificacao pos-carga falha."""
    cur = pg.cursor()
    perfil = perfil_local(con, alvo)
    antes = colunas_no_destino(cur, alvo)
    novas = sorted(set(perfil["colunas"]) - set(antes)) if antes else []
    sumidas = sorted(set(antes) - set(perfil["colunas"])) if antes else []

    print(f"\n=== {alvo.fqn} (familia {alvo.familia})")
    print(f"    local: {perfil['n_linhas']:,} linhas x {perfil['n_colunas']} colunas"
          f"  |  {len(perfil['arquivos'])} arquivos de origem")
    if antes:
        print(f"    destino ja existe com {len(antes)} colunas"
              f"  (+{len(novas)} / -{len(sumidas)})")

    if pular_iguais and ja_publicada(cur, alvo, perfil["source_hash"]):
        print("    PULADO: mesma carga ja registrada como loaded no log")
        return True

    if dry_run:
        print("    (dry-run: nada escrito no Postgres)")
        return True

    base = dict(
        execution_id=execution_id,
        familia=alvo.familia,
        staging_key=alvo.staging_key,
        target_table=alvo.fqn,
        source_file=perfil["arquivos"][0] if perfil["arquivos"] else None,
        source_hash=perfil["source_hash"],
        n_linhas=perfil["n_linhas"],
        n_colunas=perfil["n_colunas"],
        colunas_novas=json.dumps(novas),
        colunas_sumidas=json.dumps(sumidas),
        n_arquivos=len(perfil["arquivos"]) or None,
        dt_referencia_min=perfil["dt_referencia_min"],
        dt_referencia_max=perfil["dt_referencia_max"],
        modo="B",
        target="prod",
    )

    try:
        # DDL minima e a copia em streaming. `pg` e o alias do ATTACH; o
        # `cidades` local continua sendo o arquivo DuckDB.
        con.execute(f'create schema if not exists pg."{alvo.schema}"')
        con.execute(f'drop table if exists pg."{alvo.schema}"."{alvo.tabela}"')
        t0 = datetime.now()
        con.execute(
            f'create table pg."{alvo.schema}"."{alvo.tabela}" as '
            f'select * from "{alvo.schema}"."{alvo.tabela}"'
        )
        seg = (datetime.now() - t0).total_seconds()
        print(f"    copiado em {seg:.1f}s")
    except Exception as exc:  # noqa: BLE001 — o erro vai para o log e interrompe
        gravar_log(cur, status="error", error_message=str(exc)[:2000], **base)
        pg.commit()
        print(f"    ERRO na copia: {exc}")
        return False

    # --- verificacao pos-carga (6.3) ---
    cur.execute(f'select count(*) from "{alvo.schema}"."{alvo.tabela}"')
    n_destino = cur.fetchone()[0]
    # %% escapa o % do format() do Postgres para o psycopg2 nao o confundir com
    # placeholder de parametro.
    cur.execute("select pg_total_relation_size(format('%%I.%%I', %s, %s))",
                (alvo.schema, alvo.tabela))
    tamanho = cur.fetchone()[0] or 0

    if n_destino != perfil["n_linhas"]:
        msg = (f"divergencia de contagem: local {perfil['n_linhas']} "
               f"!= postgres {n_destino}")
        gravar_log(cur, status="error", error_message=msg, **base)
        pg.commit()
        print(f"    ERRO: {msg}")
        return False

    bytes_linha = tamanho / perfil["n_linhas"] if perfil["n_linhas"] else 0
    print(f"    OK: {n_destino:,} linhas conferem  |  "
          f"{tamanho / 1024**2:,.1f} MB no Postgres  ({bytes_linha:.0f} bytes/linha)")
    gravar_log(cur, status="loaded", error_message=None, **base)
    pg.commit()
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duckdb", default=os.environ.get(
        "DUCKDB_MCID_PATH", "/mnt/data/duckdb/cidades.duckdb"))
    ap.add_argument("--grupo", choices=["bronzes", "silvers", "tudo"], default="bronzes")
    ap.add_argument("--tabela", action="append", default=[],
                    help="publica so estas tabelas (schema.tabela); repetivel")
    ap.add_argument("--listar", action="store_true",
                    help="mostra a ordem de publicacao e sai")
    ap.add_argument("--dry-run", action="store_true",
                    help="mede e mostra, sem escrever no Postgres")
    ap.add_argument("--pular-iguais", action="store_true",
                    help="pula tabelas ja registradas como loaded com o mesmo "
                         "source_hash (confiavel so para as bronzes)")
    args = ap.parse_args()

    alvos = inventario(args.grupo)
    if args.tabela:
        querido = set(args.tabela)
        todos = {a.fqn: a for a in inventario("tudo")}
        faltando = querido - set(todos)
        if faltando:
            sys.exit(f"tabela fora do inventario: {sorted(faltando)}")
        alvos = [a for a in inventario("tudo") if a.fqn in querido]

    if args.listar:
        for i, a in enumerate(alvos, 1):
            print(f"{i:2}. {a.fqn:70} familia={a.familia}")
        return 0

    execution_id = str(uuid.uuid4())
    print(f"execution_id: {execution_id}")
    print(f"origem local: {args.duckdb}")
    print(f"destino     : {env('DB_DW_HOST_MCID')}/{env('DB_DW_DBNAME_MCID', 'cidades')}")

    # Read-write no arquivo local NAO porque queremos escrever nele — nada aqui
    # escreve — mas porque o DuckDB propaga o modo da conexao para os catalogos
    # ATTACHados: com read_only=True o `pg` tambem nasce read-only e o CREATE
    # TABLE do destino falha. A protecao do arquivo local fica por construcao:
    # toda escrita desta rotina e qualificada com o prefixo `pg.`.
    con = duckdb.connect(args.duckdb)
    con.execute("install postgres; load postgres")
    con.execute(f"attach '{pg_dsn()}' as pg (type postgres)")

    pg = pg_conn()
    pg.autocommit = False
    cur = pg.cursor()
    if not args.dry_run:
        garantir_log(cur)
        pg.commit()

    for alvo in alvos:
        if not publicar(con, pg, alvo, execution_id, args.dry_run, args.pular_iguais):
            print("\nPUBLICACAO INTERROMPIDA — as famílias seguintes nao foram "
                  "carregadas (ver lake._bronze_historico_log).")
            return 1

    print("\nOK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
