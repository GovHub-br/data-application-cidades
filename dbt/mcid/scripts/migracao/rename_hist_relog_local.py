#!/usr/bin/env python3
"""Rename da abreviação de tokens (`historico`->`hist`, `reloginho`->`relog`)
no `cidades.duckdb` LOCAL — equivalente DuckDB de
`renomear_hist_relog_prod.sql` (Postgres), cobrindo o mesmo `mapa_hist_relog.csv`
mais dois passos específicos do local, fora do mapa 1:1 (ver design.md,
"Correção na implementação", e README.md § Migração local):

  - `DROP TABLE prata.prata_sub50_historico_proposta` — órfã redundante de um
    rebuild anterior ao rename de arquivo da change `renomear-sub50-para-
    fnhis-historico` (mesmo conteúdo de negócio de `prata_fnhis_historico_
    proposta`, que já segue o caminho normal do mapa e vira
    `prata_hist_fnhis_proposta`);
  - `DROP TABLE bronze.bronze_shpt_sub50_propostas_apresentadas` e
    `bronze.bronze_shpt_sub50_propostas_selecionadas` — já substituídas por
    `bronze_shpt_fnhis_propostas_*`.

Sintaxe DuckDB direta (`ALTER TABLE ... RENAME TO ...`), sem `to_regclass`/
blocos `do $$` (não existem em DuckDB). Não é chamado por nenhum script de
build/publicação — execução MANUAL.

Uso:
    python3 scripts/migracao/rename_hist_relog_local.py            # forward
    python3 scripts/migracao/rename_hist_relog_local.py --rollback # reverte só o mapa 1:1 (RENAME)
    python3 scripts/migracao/rename_hist_relog_local.py --dry-run  # só mostra o plano, não executa

O rollback desfaz apenas os 34 RENAMEs — os 3 DROP (sub50 + 2 bronzes órfãs)
não são reversíveis por este script (mesma garantia dos scripts de prod: eles
nunca dropam tabela nenhuma).
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import sys

import duckdb

AQUI = pathlib.Path(__file__).resolve().parent
MAPA = AQUI / "mapa_hist_relog.csv"
DB_PATH = "/mnt/data/duckdb/cidades.duckdb"

DROPS_EXTRAS = [
    ("prata", "prata_sub50_historico_proposta"),
    ("bronze", "bronze_shpt_sub50_propostas_apresentadas"),
    ("bronze", "bronze_shpt_sub50_propostas_selecionadas"),
]


def linhas_mapa() -> list[tuple[str, str, str, str]]:
    with MAPA.open(newline="") as fh:
        linhas = list(csv.DictReader(fh))
    if not linhas:
        raise SystemExit("mapa_hist_relog.csv vazio")
    return [
        (r["old_schema"], r["old_table"], r["new_schema"], r["new_table"])
        for r in linhas
    ]


def existe(con, schema: str, tabela: str) -> bool:
    row = con.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = ? and table_name = ?",
        [schema, tabela],
    ).fetchone()
    return row[0] > 0


def contagem(con, schema: str, tabela: str) -> int:
    return con.execute(f'select count(*) from "{schema}"."{tabela}"').fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rollback", action="store_true", help="reverte só o mapa 1:1 (RENAME); não desfaz os DROP")
    ap.add_argument("--dry-run", action="store_true", help="só mostra o plano, não executa nada")
    args = ap.parse_args()

    pares = linhas_mapa()
    con = duckdb.connect(DB_PATH, read_only=args.dry_run)

    if args.rollback:
        print("== ROLLBACK: hist/relog -> historico/reloginho (só o mapa 1:1; DROPs não são desfeitos) ==")
        ops = [(ns, nt, os_, ot) for os_, ot, ns, nt in pares]
    else:
        print(f"== FORWARD: {len(pares)} tabelas do mapa + {len(DROPS_EXTRAS)} DROP extras ==")
        ops = pares

    # preflight
    erros = []
    for old_s, old_t, new_s, new_t in ops:
        if not existe(con, old_s, old_t):
            erros.append(f"origem ausente: {old_s}.{old_t}")
        if existe(con, new_s, new_t):
            erros.append(f"destino ja existe: {new_s}.{new_t}")
    if not args.rollback:
        for schema, tabela in DROPS_EXTRAS:
            if not existe(con, schema, tabela):
                print(f"aviso: {schema}.{tabela} já não existe — DROP vira no-op", file=sys.stderr)
    if erros:
        for e in erros:
            print(f"ERRO preflight: {e}", file=sys.stderr)
        return 1

    contagens_antes = {
        f"{old_s}.{old_t}": contagem(con, old_s, old_t) for old_s, old_t, _, _ in ops
    }

    if args.dry_run:
        print("-- plano (dry-run, nada executado) --")
        for old_s, old_t, new_s, new_t in ops:
            print(f'alter table "{old_s}"."{old_t}" rename to "{new_t}";  -- {contagens_antes[f"{old_s}.{old_t}"]} linhas')
        if not args.rollback:
            for schema, tabela in DROPS_EXTRAS:
                print(f'drop table if exists "{schema}"."{tabela}";')
        return 0

    con.close()
    con = duckdb.connect(DB_PATH, read_only=False)
    try:
        con.execute("begin transaction")
        for old_s, old_t, new_s, new_t in ops:
            con.execute(f'alter table "{old_s}"."{old_t}" rename to "{new_t}"')
        if not args.rollback:
            for schema, tabela in DROPS_EXTRAS:
                con.execute(f'drop table if exists "{schema}"."{tabela}"')
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise

    # postflight: contagem antes == depois, por tabela
    falhas = []
    for old_s, old_t, new_s, new_t in ops:
        esperado = contagens_antes[f"{old_s}.{old_t}"]
        if not existe(con, new_s, new_t):
            falhas.append(f"postflight: destino ausente {new_s}.{new_t}")
            continue
        obtido = contagem(con, new_s, new_t)
        if obtido != esperado:
            falhas.append(f"postflight: contagem diverge em {new_s}.{new_t}: esperado {esperado}, obtido {obtido}")
        if existe(con, old_s, old_t):
            falhas.append(f"postflight: origem ainda existe {old_s}.{old_t}")
    if not args.rollback:
        for schema, tabela in DROPS_EXTRAS:
            if existe(con, schema, tabela):
                falhas.append(f"postflight: DROP não aplicado em {schema}.{tabela}")

    if falhas:
        for f in falhas:
            print(f"ERRO postflight: {f}", file=sys.stderr)
        return 1

    print(f"ok — {len(ops)} tabela(s) renomeada(s)" + ("" if args.rollback else f" + {len(DROPS_EXTRAS)} drop(s)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
