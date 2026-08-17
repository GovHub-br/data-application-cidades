#!/usr/bin/env python3
"""Codegen: manifesto + YAML de tipos -> model dbt-duckdb da bronze.

É aqui que se resolve o "schema por família". O manifesto sabe quais arquivos compõem a
família e com que encoding/delimitador cada um deve ser lido; o YAML de tipos (o único
artefato humano) diz a semântica de cada coluna. O model sai gerado.

Dois problemas reais que a leitura precisa absorver, medidos em produção:
  - encoding varia DENTRO da família (26 de 183 famílias) -> um read_csv por grupo de
    (encoding, delimitador), unidos com UNION ALL BY NAME
  - o schema deriva (59 de 183) -> union_by_name=true dentro de cada grupo, e o YAML
    reconcilia nomes tortos via `alternativas`

`null_padding=true` entra em todo read_csv: há arquivos no lake cujo header tem menos
campos que as linhas de dados (delimitador faltando no header). Sem ele o DuckDB nem
consegue farejar o arquivo; com ele os campos excedentes aparecem como column4, column5…
e o YAML decide o que fazer com eles. O pandas, nesses casos, descarta os excedentes em
silêncio — a leitura do DuckDB preserva mais dado, não menos.
"""

import argparse
import json
import os
from pathlib import Path

import psycopg2
import yaml
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

DIR_TIPOS = RAIZ / "dbt_poc" / "tipos"
DIR_MODELS = RAIZ / "dbt_poc" / "models" / "bronze"
SCHEMA = os.environ.get("POC_SCHEMA", "manifesto")
BUCKET = os.environ["POC_MINIO_BUCKET"]

# Semântica declarada no YAML -> expressão SQL. As três primeiras chamam as macros DuckDB
# portadas dos UDFs Postgres do projeto mcid (ver dbt_poc/macros/poc_duckdb_macros.sql).
EXPRESSOES = {
    "apf": "poc_normalize_apf({col})",
    "data_br": "poc_parse_date_br({col})",
    "valor_br": "poc_parse_valor_br({col})",
    "sn_bool": "poc_sn_para_bool({col})",
    "texto": "nullif(trim({col}), '')",
    "integer": "try_cast(nullif(trim({col}), '') as integer)",
    "bigint": "try_cast(nullif(trim({col}), '') as bigint)",
    "numeric": "try_cast(nullif(trim({col}), '') as decimal(18, 2))",
    "date": "try_cast(nullif(trim({col}), '') as date)",
    "timestamp": "try_cast(nullif(trim({col}), '') as timestamp)",
}


def conectar():
    return psycopg2.connect(
        host=os.environ["POC_PG_HOST"], port=os.environ["POC_PG_PORT"],
        user=os.environ["POC_PG_USER"], password=os.environ["POC_PG_PASSWORD"],
        dbname=os.environ["POC_PG_DBNAME"],
    )


def grupos_da_familia(conn, familia: str, chaves: "set | None" = None) -> list:
    """[(encoding, delimitador, header_ok, [keys...])] — um grupo por combinação de leitura.

    `header_ok` entra na chave de agrupamento porque arquivos cujo header é inutilizável
    precisam ser lidos com header=false + nomes explícitos, o que não pode ser misturado
    no mesmo read_csv dos arquivos bem-formados.

    `chaves` é o gancho do incremental: quando informado, só esses minio_keys entram.
    Em produção a lista sai da mesma comparação (minio_key, source_hash) do _staging_log.

    Arquivos com `chave_leitura` preenchida (ver gerar_manifesto.reescrever_utf8) tiveram
    bytes C1 fora de UTF-8 válido e foram reescritos numa cópia UTF-8 à parte: o grupo lê
    essa cópia, sempre com encoding='utf-8', em vez do `minio_key` original — que é
    exatamente o que ficou irrecuperável para o DuckDB. O filtro por `chaves` continua
    comparando o `minio_key` original (é essa a chave que o controle de incremental conhece).
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                CASE WHEN chave_leitura IS NOT NULL THEN 'utf-8' ELSE encoding END,
                delimitador, header_ok,
                array_agg(minio_key ORDER BY minio_key),
                array_agg(COALESCE(chave_leitura, minio_key) ORDER BY minio_key)
            FROM {SCHEMA}.{TABELA_MANIFESTO}
            WHERE familia = %s AND legivel_duckdb
            GROUP BY 1, delimitador, header_ok
            ORDER BY count(*) DESC
        """, (familia,))
        grupos = []
        for e, d, hok, mks, cks in cur.fetchall():
            pares = [(mk, ck) for mk, ck in zip(mks, cks) if chaves is None or mk in chaves]
            if pares:
                grupos.append((e, d, hok, [ck for _, ck in pares]))
        return grupos


TABELA_MANIFESTO = "_manifesto_bronze"


def sql_leitura(grupos: list, canonicas: list) -> str:
    """UNION ALL BY NAME de um read_csv por grupo de (encoding, delimitador, header_ok)."""
    partes = []
    for encoding, delim, header_ok, keys in grupos:
        lista = ",\n            ".join(f"'s3://{BUCKET}/{k}'" for k in keys)
        delim_sql = delim.replace("\\", "\\\\").replace("'", "''")
        if header_ok:
            cabecalho = "header=true"
            nomes = ""
            nota = ""
        else:
            # Header separado pelo delimitador errado: descartado com skip=1 e substituído
            # pelos nomes canônicos declarados no YAML da família.
            cabecalho = "header=false, skip=1"
            nomes = "        names=[" + ", ".join(f"'{c}'" for c in canonicas) + "],\n"
            nota = " [header inutilizável: nomes vindos do YAML]"
        partes.append(
            f"    -- {len(keys)} arquivo(s): encoding={encoding}, delim={delim!r}{nota}\n"
            f"    select * from read_csv(\n"
            f"        [\n            {lista}\n        ],\n"
            f"{nomes}"
            f"        delim='{delim_sql}', {cabecalho}, all_varchar=true,\n"
            f"        encoding='{encoding}', union_by_name=true, null_padding=true,\n"
            f"        filename=true, normalize_names=true\n"
            f"    )"
        )
    return "\n    union all by name\n".join(partes)


def sql_colunas(spec: dict, colunas_vistas: set) -> str:
    """SELECT tipado. `alternativas` reconcilia nomes que derivaram entre arquivos."""
    linhas = []
    for nome, cfg in spec["colunas"].items():
        tipo = cfg["tipo"]
        molde = EXPRESSOES.get(tipo)
        if molde is None:
            raise SystemExit(f"tipo desconhecido {tipo!r} em {nome!r}")

        fontes = [nome] + [a for a in cfg.get("alternativas", []) if a in colunas_vistas]
        fontes = [f for f in fontes if f in colunas_vistas]
        if not fontes:
            expr = "null"
        elif len(fontes) == 1:
            expr = molde.format(col=fontes[0])
        else:
            # coalesce ANTES do cast: o valor certo pode estar em qualquer das variantes
            expr = molde.format(col=f"coalesce({', '.join(fontes)})")
        linhas.append(f"    {expr:<62} as {nome},")
    return "\n".join(linhas)


def colunas_da_familia(conn, familia: str, canonicas: list,
                       chaves: "set | None" = None) -> set:
    """Colunas que existirão no `from fonte` depois da união dos grupos DESTE lote.

    O filtro por `chaves` é obrigatório, não uma otimização: se o conjunto fosse o da
    família inteira, um lote incremental pequeno geraria SELECT referenciando colunas que
    só existem em arquivos fora do lote, e o model quebraria com "column not found".

    Para os arquivos de header inutilizável o header do manifesto é lixo: eles são lidos
    com os nomes canônicos do YAML, então é isso que entra no conjunto.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT minio_key, header, header_ok FROM {SCHEMA}.{TABELA_MANIFESTO}
            WHERE familia = %s AND header IS NOT NULL AND legivel_duckdb
        """, (familia,))
        vistas: set = set()
        for minio_key, header, header_ok in cur.fetchall():
            if chaves is not None and minio_key not in chaves:
                continue
            if not header_ok:
                vistas.update(canonicas)
                continue
            vistas.update(header if isinstance(header, list) else json.loads(header))
        return vistas


def gerar(conn, caminho_yml: Path, chaves: "set | None" = None) -> Path:
    spec = yaml.safe_load(caminho_yml.read_text(encoding="utf-8"))
    familia, tabela = spec["familia"], spec["tabela"]
    particao = spec.get("particao")

    grupos = grupos_da_familia(conn, familia, chaves)
    if not grupos:
        raise SystemExit(f"nenhum arquivo legível para a família {familia!r}")
    vistas = colunas_da_familia(conn, familia, list(spec['colunas']), chaves)

    n_arquivos = sum(len(g[3]) for g in grupos)
    opcoes = ["'format': 'parquet'", "'compression': 'snappy'"]
    if particao:
        # partition_by + overwrite_or_ignore: cada run reescreve só as partições tocadas,
        # que é o que torna o external materialization viável com centenas de arquivos.
        opcoes += [f"'partition_by': '{particao}'", "'overwrite_or_ignore': 1"]

    sql = f"""{{{{ config(
    materialized='external',
    location='s3://{BUCKET}/bronze/{tabela}',
    options={{{', '.join(opcoes)}}}
) }}}}

-- GERADO por scripts/gerar_models_bronze.py a partir de:
--   {SCHEMA}.{TABELA_MANIFESTO}  (quais arquivos, com que encoding/delimitador)
--   tipos/{caminho_yml.name}  (a semântica de cada coluna — o artefato humano)
-- Não editar à mão: regenerar.
--
-- Família {familia!r}: {n_arquivos} arquivo(s) em {len(grupos)} grupo(s) de leitura.

with fonte as (

{sql_leitura(grupos, list(spec['colunas']))}

)

select
{sql_colunas(spec, vistas)}
    filename                                                   as _source_file,
    cast('{{{{ var("ingested_at") }}}}' as timestamp)              as _ingested_at
from fonte
"""
    DIR_MODELS.mkdir(parents=True, exist_ok=True)
    destino = DIR_MODELS / f"bronze_{tabela}.sql"
    destino.write_text(sql, encoding="utf-8")

    print(f"  {destino.relative_to(RAIZ)}")
    print(f"    {n_arquivos} arquivo(s), {len(grupos)} grupo(s) de leitura:")
    for enc, delim, hok, keys in grupos:
        marca = "" if hok else "  [header do YAML]"
        print(f"      {len(keys):3d}x  encoding={enc:8s} delim={delim!r}{marca}")
    extras = vistas - set(spec["colunas"]) - set(spec.get("ignorar", []))
    extras -= {a for c in spec["colunas"].values() for a in c.get("alternativas", [])}
    if extras:
        print(f"    ⚠ colunas vistas nos arquivos e ausentes do YAML: {sorted(extras)}")
    return destino


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--familia", help="Gera só esta família (default: todas com YAML).")
    p.add_argument("--lote", metavar="ARQUIVO",
                   help="Arquivo com um minio_key por linha: gera o model só com esses. "
                        "É o gancho do incremental — em produção a lista vem da comparação "
                        "(minio_key, source_hash) contra o que já foi processado.")
    args = p.parse_args()

    ymls = sorted(DIR_TIPOS.glob("*.yml"))
    if args.familia:
        ymls = [y for y in ymls if y.stem == args.familia]
    if not ymls:
        raise SystemExit(f"nenhum YAML de tipos em {DIR_TIPOS}")

    chaves = None
    if args.lote:
        chaves = {
            l.strip() for l in Path(args.lote).read_text(encoding="utf-8").splitlines()
            if l.strip()
        }
        print(f"lote com {len(chaves)} arquivo(s)")

    conn = conectar()
    print(f"Gerando {len(ymls)} model(s) bronze:\n")
    for y in ymls:
        gerar(conn, y, chaves)
    conn.close()


if __name__ == "__main__":
    main()
