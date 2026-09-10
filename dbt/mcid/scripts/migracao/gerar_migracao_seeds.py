#!/usr/bin/env python3
"""Gera os 3 scripts SQL de migração das SEEDS do eixo histórico/reloginho para
o schema `seeds` a partir do `mapa_seeds.csv`.

    python3 scripts/migracao/gerar_migracao_seeds.py

Emite, no mesmo diretório:
  - migrar_seeds_prod.sql      (forward: data_quality / conjuntura -> seeds)
  - reverter_seeds_prod.sql    (rollback: seeds -> data_quality / conjuntura)
  - verificar_seeds_prod.sql   (read-only: inventário + contagens)

Diferente da migração de modelos (gerar_migracao.py), as seeds NÃO mudam de
nome — apenas de schema (`ALTER TABLE ... SET SCHEMA seeds`), sem RENAME. As
seeds migradas são exatamente as consumidas pelos braços histórico
(mcmv_historico_dbt) e reloginho/gargalo (indicadores_mcmv_dbt).

Rodado UMA vez pelo implementador; os `.sql` gerados são commitados e revisados
linha a linha. O operador em produção usa só `psql` — não precisa de Python.
NENHUM destes scripts é chamado por automação; a execução contra `prod` é ato
MANUAL, fora da aplicação da change. Ver README.md.
"""
from __future__ import annotations

import csv
import pathlib

AQUI = pathlib.Path(__file__).resolve().parent
MAPA = AQUI / "mapa_seeds.csv"

CABECALHO = """\
-- =====================================================================
-- {titulo}
--
-- GERADO por scripts/migracao/gerar_migracao_seeds.py a partir de mapa_seeds.csv.
-- NÃO EDITAR À MÃO — regenerar com o script.
--
-- ATENÇÃO: script MANUAL, roda contra o Postgres `prod`. NÃO deve ser chamado
-- por publicar_historico.py, publicar-historico.sh, run-*.sh, rebuild-local.sh
-- nem por hooks on-run-* do dbt. Move as SEEDS do eixo histórico/reloginho para
-- o schema `seeds` (METADADO — instantâneo, não copia dados). As seeds não
-- mudam de nome, só de schema.
--
-- Uso:
--   psql "$DSN" -v ON_ERROR_STOP=1 -f {arquivo}
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\\set ON_ERROR_STOP on
"""


def pares() -> list[tuple[str, str, str, str]]:
    """Lê o mapa e devolve 4-tuplas (old_schema, old_table, new_schema,
    new_table) com new_table == old_table (as seeds não mudam de nome)."""
    with MAPA.open(newline="") as fh:
        linhas = list(csv.DictReader(fh))
    if not linhas:
        raise SystemExit("mapa_seeds.csv vazio")
    return [
        (r["old_schema"], r["old_table"], r["new_schema"], r["old_table"])
        for r in linhas
    ]


def _checa(p, *, schema_i: int, table_i: int, existe: bool, msg: str) -> str:
    cond = "is null" if existe else "is not null"
    return (
        f"    if to_regclass('{p[schema_i]}.{p[table_i]}') {cond} then\n"
        f"        raise exception '{msg}: %', '{p[schema_i]}.{p[table_i]}';\n"
        f"    end if;"
    )


def bloco_preflight(pares, *, reverso: bool) -> str:
    o_s, o_t = (2, 3) if reverso else (0, 1)
    d_s, d_t = (0, 1) if reverso else (2, 3)
    checa_origem = "\n".join(
        _checa(p, schema_i=o_s, table_i=o_t, existe=True, msg="origem ausente")
        for p in pares
    )
    checa_destino = "\n".join(
        _checa(p, schema_i=d_s, table_i=d_t, existe=False, msg="destino ja existe")
        for p in pares
    )
    return (
        "do $$\nbegin\n"
        f"    -- preflight: {len(pares)} seeds no inventario\n"
        f"{checa_origem}\n{checa_destino}\n"
        "end $$;\n"
    )


def bloco_mover(pares, *, reverso: bool) -> str:
    out = []
    for old_s, old_t, new_s, _ in pares:
        if reverso:
            old_s, new_s = new_s, old_s
        # as seeds não mudam de nome — só SET SCHEMA (sem RENAME).
        out.append(f'alter table "{old_s}"."{old_t}" set schema "{new_s}";')
    return "\n".join(out) + "\n"


def bloco_postflight(pares, *, reverso: bool) -> str:
    d_s, d_t = (0, 1) if reverso else (2, 3)
    o_s, o_t = (2, 3) if reverso else (0, 1)
    checa_destino = "\n".join(
        _checa(p, schema_i=d_s, table_i=d_t, existe=True,
               msg="postflight: destino ausente")
        for p in pares
    )
    checa_origem = "\n".join(
        _checa(p, schema_i=o_s, table_i=o_t, existe=False,
               msg="postflight: origem ainda existe")
        for p in pares
    )
    return "do $$\nbegin\n" + checa_destino + "\n" + checa_origem + "\nend $$;\n"


def gera(pares, *, reverso: bool, titulo: str, arquivo: str) -> str:
    partes = [
        CABECALHO.format(titulo=titulo, arquivo=f"scripts/migracao/{arquivo}"),
        "begin;\n",
        "-- 1) preflight -------------------------------------------------------",
        bloco_preflight(pares, reverso=reverso),
        "-- 2) move de schema (SET SCHEMA; as seeds não mudam de nome) --------",
        bloco_mover(pares, reverso=reverso),
        "-- 3) postflight -----------------------------------------------------",
        bloco_postflight(pares, reverso=reverso),
        "commit;",
        "",
    ]
    return "\n".join(partes)


def gera_verificar(pares) -> str:
    schemas = sorted({p[0] for p in pares} | {p[2] for p in pares})
    lista_schemas = ", ".join(f"'{s}'" for s in schemas)
    counts = "\nunion all\n".join(
        f"select '{s}.{t}' as tabela, "
        f"(select count(*) from \"{s}\".\"{t}\") as n_linhas"
        for s, t in [(p[0], p[1]) for p in pares]
    )
    counts_novo = "\nunion all\n".join(
        f"select '{s}.{t}' as tabela, "
        f"(select count(*) from \"{s}\".\"{t}\") as n_linhas"
        for s, t in [(p[2], p[3]) for p in pares]
    )
    return f"""\
-- =====================================================================
-- verificar_seeds_prod.sql — READ-ONLY. Rodar ANTES e DEPOIS da migração
-- das seeds e diffar as saídas.
--
-- GERADO por scripts/migracao/gerar_migracao_seeds.py — NÃO EDITAR À MÃO.
--   psql "$DSN" -f scripts/migracao/verificar_seeds_prod.sql
-- =====================================================================

\\echo '== seeds presentes nos schemas de origem e destino =='
select table_schema, table_name
from information_schema.tables
where table_schema in ({lista_schemas})
order by table_schema, table_name;

\\echo '== contagem por seed — schemas ANTIGOS (deve existir ANTES) =='
select * from (
{counts}
) t order by tabela;

\\echo '== contagem por seed — schema `seeds` (deve existir DEPOIS) =='
select * from (
{counts_novo}
) t order by tabela;
"""


def main() -> int:
    p = pares()
    (AQUI / "migrar_seeds_prod.sql").write_text(
        gera(
            p,
            reverso=False,
            titulo="migrar_seeds_prod.sql — data_quality/conjuntura -> seeds",
            arquivo="migrar_seeds_prod.sql",
        )
    )
    (AQUI / "reverter_seeds_prod.sql").write_text(
        gera(
            p,
            reverso=True,
            titulo="reverter_seeds_prod.sql — ROLLBACK: seeds -> data_quality/conjuntura",
            arquivo="reverter_seeds_prod.sql",
        )
    )
    (AQUI / "verificar_seeds_prod.sql").write_text(gera_verificar(p))
    print(f"gerados 3 .sql a partir de {len(p)} seeds do mapa")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
