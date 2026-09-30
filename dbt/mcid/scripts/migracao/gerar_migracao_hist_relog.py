#!/usr/bin/env python3
"""Gera os 3 scripts SQL de migração da abreviação de tokens em produção a
partir do `mapa_hist_relog.csv`.

    python3 scripts/migracao/gerar_migracao_hist_relog.py

Emite, no mesmo diretório:
  - renomear_hist_relog_prod.sql   (forward: historico/reloginho -> hist/relog)
  - reverter_hist_relog_prod.sql   (rollback: o inverso exato)
  - verificar_hist_relog_prod.sql  (read-only: inventário + contagens)

Irmão dedicado de `gerar_migracao.py` (change `renomear-camadas-pt-historico-
reloginho`, D1 de `renomear-hist-relog-prata-ouro`): aqui `old_schema ==
new_schema` sempre (rename dentro do mesmo schema prata/ouro) — por isso não há
passo `SET SCHEMA`, diferente do gerador anterior. Rodado UMA vez pelo
implementador da change; os `.sql` gerados são commitados e revisados linha a
linha. O operador em produção usa só `psql` — não precisa de Python. NENHUM
destes scripts é chamado por automação; a execução contra `prod` é ato MANUAL,
fora da aplicação da change. Ver README.md.
"""
from __future__ import annotations

import csv
import pathlib

AQUI = pathlib.Path(__file__).resolve().parent
MAPA = AQUI / "mapa_hist_relog.csv"

CABECALHO = """\
-- =====================================================================
-- {titulo}
--
-- GERADO por scripts/migracao/gerar_migracao_hist_relog.py a partir de
-- mapa_hist_relog.csv. NÃO EDITAR À MÃO — regenerar com o script.
--
-- ATENÇÃO: script MANUAL, roda contra o Postgres `prod`. NÃO deve ser chamado
-- por publicar_historico.py, publicar-historico.sh, run-*.sh, rebuild-local.sh
-- nem por hooks on-run-* do dbt. Renomeia METADADO (ALTER TABLE ... RENAME TO,
-- dentro do MESMO schema) — instantâneo, não copia dados. Consumidores que
-- referenciam as tabelas por string de nome (Superset / OpenMetadata /
-- notebooks) quebram — usuário já confirmou que o inventário abaixo é seguro.
--
-- Uso:
--   psql "$DSN" -v ON_ERROR_STOP=1 -f {arquivo}
-- (ordem e pré-requisitos: ver README.md)
-- =====================================================================

\\set ON_ERROR_STOP on
"""


def linhas_mapa() -> list[dict]:
    with MAPA.open(newline="") as fh:
        linhas = list(csv.DictReader(fh))
    if not linhas:
        raise SystemExit("mapa_hist_relog.csv vazio")
    for r in linhas:
        if r["old_schema"] != r["new_schema"]:
            raise SystemExit(
                f"linha com old_schema != new_schema ({r}) — este gerador só "
                "cobre rename dentro do mesmo schema; use gerar_migracao.py "
                "para mudança de schema"
            )
    return linhas


def bloco_preflight(pares: list[tuple[str, str, str, str]], reverso: bool) -> str:
    origem_i, origem_t = (2, 3) if reverso else (0, 1)
    destino_i, destino_t = (0, 1) if reverso else (2, 3)
    checa_origem = "\n".join(
        f"    if to_regclass('{p[origem_i]}.{p[origem_t]}') is null then\n"
        f"        raise exception 'origem ausente: %', '{p[origem_i]}.{p[origem_t]}';\n"
        f"    end if;"
        for p in pares
    )
    checa_destino = "\n".join(
        f"    if to_regclass('{p[destino_i]}.{p[destino_t]}') is not null then\n"
        f"        raise exception 'destino ja existe: %', '{p[destino_i]}.{p[destino_t]}';\n"
        f"    end if;"
        for p in pares
    )
    return (
        "do $$\nbegin\n"
        f"    -- preflight: {len(pares)} tabelas no inventario\n"
        f"{checa_origem}\n{checa_destino}\n"
        "end $$;\n"
    )


def bloco_renomeia(pares: list[tuple[str, str, str, str]], reverso: bool) -> str:
    out = []
    for old_s, old_t, new_s, new_t in pares:
        if reverso:
            old_s, old_t, new_s, new_t = new_s, new_t, old_s, old_t
        # old_schema == new_schema sempre neste mapa — só RENAME, sem SET SCHEMA.
        out.append(f'alter table "{old_s}"."{old_t}" rename to "{new_t}";')
    return "\n".join(out) + "\n"


def bloco_postflight(pares: list[tuple[str, str, str, str]], reverso: bool) -> str:
    destino_i, destino_t = (0, 1) if reverso else (2, 3)
    origem_i, origem_t = (2, 3) if reverso else (0, 1)
    checa = "\n".join(
        f"    if to_regclass('{p[destino_i]}.{p[destino_t]}') is null then\n"
        f"        raise exception 'postflight: destino ausente %', '{p[destino_i]}.{p[destino_t]}';\n"
        f"    end if;\n"
        f"    if to_regclass('{p[origem_i]}.{p[origem_t]}') is not null then\n"
        f"        raise exception 'postflight: origem ainda existe %', '{p[origem_i]}.{p[origem_t]}';\n"
        f"    end if;"
        for p in pares
    )
    return "do $$\nbegin\n" + checa + "\nend $$;\n"


def gera(pares, *, reverso: bool, titulo: str, arquivo: str) -> str:
    partes = [
        CABECALHO.format(titulo=titulo, arquivo=f"scripts/migracao/{arquivo}"),
        "begin;\n",
        "-- 1) preflight -------------------------------------------------------",
        bloco_preflight(pares, reverso),
        "-- 2) renomeia (mesmo schema — sem SET SCHEMA) ------------------------",
        bloco_renomeia(pares, reverso),
        "-- 3) postflight -----------------------------------------------------",
        bloco_postflight(pares, reverso),
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
        f"select '{ns}.{nt}' as tabela, "
        f"(select count(*) from \"{ns}\".\"{nt}\") as n_linhas"
        for ns, nt in [(p[2], p[3]) for p in pares]
    )
    return f"""\
-- =====================================================================
-- verificar_hist_relog_prod.sql — READ-ONLY. Rodar ANTES e DEPOIS da
-- migração e diffar as saídas.
--
-- GERADO por scripts/migracao/gerar_migracao_hist_relog.py — NÃO EDITAR À MÃO.
--   psql "$DSN" -f scripts/migracao/verificar_hist_relog_prod.sql
-- =====================================================================

\\echo '== tabelas presentes nos schemas prata/ouro cujo nome contem historico/reloginho ou hist/relog =='
select table_schema, table_name
from information_schema.tables
where table_schema in ({lista_schemas})
    and (table_name like '%historico%' or table_name like '%reloginho%'
         or table_name like '%hist%' or table_name like '%relog%')
order by table_schema, table_name;

\\echo '== contagem por tabela — nomes ANTIGOS (deve existir ANTES da migração) =='
select * from (
{counts}
) t order by tabela;

\\echo '== contagem por tabela — nomes NOVOS (deve existir DEPOIS da migração) =='
select * from (
{counts_novo}
) t order by tabela;
"""


def main() -> int:
    linhas = linhas_mapa()
    pares = [
        (r["old_schema"], r["old_table"], r["new_schema"], r["new_table"])
        for r in linhas
    ]

    (AQUI / "renomear_hist_relog_prod.sql").write_text(
        gera(pares, reverso=False,
             titulo="renomear_hist_relog_prod.sql — historico/reloginho -> hist/relog (mesmo schema)",
             arquivo="renomear_hist_relog_prod.sql")
    )
    (AQUI / "reverter_hist_relog_prod.sql").write_text(
        gera(pares, reverso=True,
             titulo="reverter_hist_relog_prod.sql — ROLLBACK: hist/relog -> historico/reloginho",
             arquivo="reverter_hist_relog_prod.sql")
    )
    (AQUI / "verificar_hist_relog_prod.sql").write_text(gera_verificar(pares))
    print(f"gerados 3 .sql a partir de {len(pares)} linhas do mapa")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
