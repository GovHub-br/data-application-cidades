"""Boletim de Conjuntura no Superset: definição dos quadros + construção.

Um arquivo só, em duas metades:

  1. **O que cada página mostra** — `pagina_01()` … `pagina_07()`, cada uma
     devolvendo os quadros daquela página do boletim. É aqui que se inclui ou
     tira um chart; nada mais precisa mudar.
  2. **Como isso vira dashboard** — sessão do Superset, dataset virtual por
     quadro, chart e layout em abas com filtro de trimestre.

Uso:

    poetry run python scripts/superset/build_boletim.py                # tudo
    poetry run python scripts/superset/build_boletim.py --paginas 3,5  # só 3 e 5
    poetry run python scripts/superset/build_boletim.py --dry-run

Reconstruir uma página não apaga as outras: o layout das não selecionadas é
preservado a partir do que já está publicado.

Um único dashboard, com filtro de edição. Cada quadro devolve uma linha por
edição possível, e o filtro nativo (coluna ``edicao``) escolhe o trimestre —
em vez de um dashboard por trimestre.

**Consequência de projeto, deliberada.** No boletim impresso os cabeçalhos são
literais ("2025 1ºTri", "X 4T25"). Aqui eles são RELATIVOS ao trimestre
escolhido ("trim. anterior", "12m atual"), porque no Superset o nome da coluna
faz parte do schema do dataset e não muda conforme o filtro. O significado é o
mesmo do impresso; a legenda é que deixa de ser literal.

Índice linear de trimestre: ``k = ano * 4 + trimestre``. Assim o trimestre
anterior é ``k-1``, o mesmo trimestre do ano anterior é ``k-4``, a janela de 12
meses é ``k-3..k`` e a janela anterior é ``k-7..k-4``. Para as séries mensais,
``m = ano * 12 + mes``, e o mês de referência é o último do trimestre.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass, field

import requests

MART = "conjuntura_continuo_mart"
SILVER = "conjuntura_continuo_silver"

#: primeira edição oferecida no filtro. Antes disso, as séries mensais
#: (CAGED e produção física começam em 2024-01) não têm 12 meses de retaguarda.
PRIMEIRO_ANO = 2025


@dataclass(frozen=True)
class Quadro:
    pagina: int
    secao: str
    titulo: str
    sql: str
    colunas: list[str] = field(default_factory=list)
    ordenar: str = "ordem"
    nota: str = ""


# --------------------------------------------------------------------------
# Blocos reutilizados

#: edições disponíveis, a partir dos trimestres que existem no gold de PIB
EDICOES = f"""
edicoes as (
    select distinct periodo as edicao,
           (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
           right(periodo, 4)::int as ano_ed,
           left(periodo, 1)::int  as tri_ed
    from {MART}.gold_continuo_pib_construcao_civil_pct
    where right(periodo, 4)::int >= {PRIMEIRO_ANO}
)"""



def num(coluna: str) -> str:
    """Cast numérico tolerante para as colunas `text` dos balanços.

    Elas trazem células vazias e espaços não separáveis (U+00A0), que o
    `btrim` padrão não remove — um cast direto estoura com
    'invalid input syntax for type numeric: " "'. O regex garante que só vira
    número o que de fato é número; o resto vira NULL, que é o comportamento
    correto para célula em branco na planilha de origem.
    """
    limpo = f"btrim({coluna}::text, E' \\t\\r\\n\\u00a0')"
    return (f"(case when {limpo} ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' "
            f"then {limpo}::numeric end)")


def _serie_trimestral(tabela: str, colunas: str) -> str:
    return f"""
serie as (
    select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k, {colunas}
    from {MART}.{tabela}
)"""


def pagina_01() -> list[Quadro]:
    """Página 1 do boletim."""
    return [
        Quadro(
            pagina=1,
            secao="1. PIB da Construção Civil",
            titulo="PIB Construção Civil (em % de Crescimento)",
            colunas=["indicador", "4 trim. antes", "3 trim. antes", "2 trim. antes",
                     "trim. anterior", "trimestre selecionado"],
            sql=f"""
    with {EDICOES},
    serie as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               var_trim_trim_anterior      as v1,
               var_acumulada_ano           as v2,
               var_acumulada_4_trimestres  as v3
        from {MART}.gold_continuo_pib_construcao_civil_pct
    ),
    m as (
        select e.edicao, e.k,
               1 as ordem, 'Trim./Trim. Imediatamente Anterior' as indicador,
               (select v1 from serie where k = e.k - 4) as c4,
               (select v1 from serie where k = e.k - 3) as c3,
               (select v1 from serie where k = e.k - 2) as c2,
               (select v1 from serie where k = e.k - 1) as c1,
               (select v1 from serie where k = e.k)     as c0
        from edicoes e
        union all
        select e.edicao, e.k, 2, 'Acumulada ao Longo do Ano',
               (select v2 from serie where k = e.k - 4), (select v2 from serie where k = e.k - 3),
               (select v2 from serie where k = e.k - 2), (select v2 from serie where k = e.k - 1),
               (select v2 from serie where k = e.k)
        from edicoes e
        union all
        select e.edicao, e.k, 3, 'Acum. Últimos 4 Trimestres',
               (select v3 from serie where k = e.k - 4), (select v3 from serie where k = e.k - 3),
               (select v3 from serie where k = e.k - 2), (select v3 from serie where k = e.k - 1),
               (select v3 from serie where k = e.k)
        from edicoes e
    )
    select edicao, indicador,
           c4 as "4 trim. antes", c3 as "3 trim. antes", c2 as "2 trim. antes",
           c1 as "trim. anterior", c0 as "trimestre selecionado", ordem
    from m
    """,
        ),
        # --- Seção 2: CBIC. Vem da inserção manual em lote (script 0003), não de
        # ingestão automática — a base da CBIC não é acessível por API.
        Quadro(
            pagina=1,
            secao="2. Lançamentos e Vendas",
            titulo="Lançamentos por Região (CBIC)",
            colunas=["regiao", "TOTAL", "MCMV", "% MCMV"],
            sql=f"""
    with {EDICOES},
    d as (select periodo, periodo from manual_conjuntura.dados_trimestrais)
    select e.edicao, x.regiao, x.total as "TOTAL", x.mcmv as "MCMV",
           round((x.mcmv / nullif(x.total, 0) * 100)::numeric, 0) as "% MCMV", x.ordem
    from edicoes e
    join manual_conjuntura.dados_trimestrais d on d.periodo = e.edicao
    cross join lateral (
        select 'NORTE' as regiao, 1 as ordem, {num('d.cbic_lancamentos_total_n')} total, {num('d.cbic_lancamentos_mcmv_n')} mcmv
        union all select 'NORDESTE', 2, {num('d.cbic_lancamentos_total_ne')}, {num('d.cbic_lancamentos_mcmv_ne')}
        union all select 'CENTRO-OESTE', 3, {num('d.cbic_lancamentos_total_co')}, {num('d.cbic_lancamentos_mcmv_co')}
        union all select 'SUDESTE', 4, {num('d.cbic_lancamentos_total_se')}, {num('d.cbic_lancamentos_mcmv_se')}
        union all select 'SUL', 5, {num('d.cbic_lancamentos_total_s')}, {num('d.cbic_lancamentos_mcmv_s')}
    ) x
    """,
        ),
        Quadro(
            pagina=1,
            secao="2. Lançamentos e Vendas",
            titulo="Vendas por Região (CBIC)",
            colunas=["regiao", "TOTAL", "MCMV", "% MCMV"],
            sql=f"""
    with {EDICOES}
    select e.edicao, x.regiao, x.total as "TOTAL", x.mcmv as "MCMV",
           round((x.mcmv / nullif(x.total, 0) * 100)::numeric, 0) as "% MCMV", x.ordem
    from edicoes e
    join manual_conjuntura.dados_trimestrais d on d.periodo = e.edicao
    cross join lateral (
        select 'NORTE' as regiao, 1 as ordem, {num('d.cbic_vendas_total_n')} total, {num('d.cbic_vendas_mcmv_n')} mcmv
        union all select 'NORDESTE', 2, {num('d.cbic_vendas_total_ne')}, {num('d.cbic_vendas_mcmv_ne')}
        union all select 'CENTRO-OESTE', 3, {num('d.cbic_vendas_total_co')}, {num('d.cbic_vendas_mcmv_co')}
        union all select 'SUDESTE', 4, {num('d.cbic_vendas_total_se')}, {num('d.cbic_vendas_mcmv_se')}
        union all select 'SUL', 5, {num('d.cbic_vendas_total_s')}, {num('d.cbic_vendas_mcmv_s')}
    ) x
    """,
        ),
        Quadro(
            pagina=1,
            secao="2. Lançamentos e Vendas",
            titulo="CBIC — Lançamentos e Vendas (totais)",
            colunas=["periodo", "Lançamentos TOTAL", "Lançamentos MCMV", "Lançamentos DEMAIS",
                     "Vendas TOTAL", "Vendas MCMV", "Vendas DEMAIS"],
            sql=f"""
    with {EDICOES},
    s as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               {num('cbic_lancamentos_total')} lt, {num('cbic_lancamentos_mcmv')} lm,
               {num('cbic_vendas_total')} vt, {num('cbic_vendas_mcmv')} vm,
               {num('cbic_lancamentos_total_acumulado_12_meses')} lt12,
               {num('cbic_lancamentos_mcmv_acumulado_12_meses')} lm12,
               {num('cbic_vendas_total_acumulado_12_meses')} vt12,
               {num('cbic_vendas_mcmv_acumulado_12_meses')} vm12
        from manual_conjuntura.dados_trimestrais
        where periodo ~ '^[1-4]T[0-9]{{4}}$'
    )
    select e.edicao, x.rotulo as periodo,
           x.lt as "Lançamentos TOTAL", x.lm as "Lançamentos MCMV", x.lt - x.lm as "Lançamentos DEMAIS",
           x.vt as "Vendas TOTAL", x.vm as "Vendas MCMV", x.vt - x.vm as "Vendas DEMAIS", x.ordem
    from edicoes e
    cross join lateral (
        select 'Trimestre selecionado' as rotulo, 1 as ordem, lt, lm, vt, vm from s where k = e.k
        union all select 'Trimestre anterior', 2, lt, lm, vt, vm from s where k = e.k - 1
        union all select 'Mesmo trim. do ano anterior', 3, lt, lm, vt, vm from s where k = e.k - 4
        union all select '12 meses até a referência', 4, lt12, lm12, vt12, vm12 from s where k = e.k
        union all select '12 meses anteriores', 5, lt12, lm12, vt12, vm12 from s where k = e.k - 4
    ) x
    """,
            nota="Inserção manual em lote (script 0003). A CBIC revisa trimestres já publicados.",
        ),
    ]


def pagina_02() -> list[Quadro]:
    """Página 2 do boletim."""
    return [
        Quadro(
            pagina=2,
            secao="3. Balanços das Empresas",
            titulo="Lançamentos por construtora (variação %)",
            colunas=["empresa", "vs. trim. anterior", "vs. mesmo trim. ano ant.",
                     "12m atual / 12m anterior", "12m anterior / 12m retrasado"],
            sql=f"""
    with {EDICOES},
    serie as (
        select empresa, (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               lancamentos::numeric as v,
               {num('var_lancamentos_tri_anterior')}            as var_tri,
               {num('var_lancamentos_mesmo_tri_ano_anterior')}  as var_ano
        from {MART}.gold_continuo_balancos_empresas
    )
    select e.edicao, s.empresa,
           round(s.var_tri * 100, 0) as "vs. trim. anterior",
           round(s.var_ano * 100, 0) as "vs. mesmo trim. ano ant.",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 3 and e.k)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0)
                as "12m atual / 12m anterior",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0)
                as "12m anterior / 12m retrasado",
           case s.empresa when 'MRV' then 1 when 'Cury' then 2 when 'Tenda' then 3
                when 'Direcional' then 4 when 'Pacaembu' then 5 else 6 end as ordem
    from edicoes e join serie s on s.k = e.k
    """,
        ),
        Quadro(
            pagina=2,
            secao="3. Balanços das Empresas",
            titulo="Vendas por construtora (variação %)",
            colunas=["empresa", "vs. trim. anterior", "vs. mesmo trim. ano ant.",
                     "12m atual / 12m anterior", "12m anterior / 12m retrasado"],
            sql=f"""
    with {EDICOES},
    serie as (
        select empresa, (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               vendas::numeric as v,
               {num('var_vendas_tri_anterior')}            as var_tri,
               {num('var_vendas_mesmo_tri_ano_anterior')}  as var_ano
        from {MART}.gold_continuo_balancos_empresas
    )
    select e.edicao, s.empresa,
           round(s.var_tri * 100, 0) as "vs. trim. anterior",
           round(s.var_ano * 100, 0) as "vs. mesmo trim. ano ant.",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 3 and e.k)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0)
                as "12m atual / 12m anterior",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0)
                as "12m anterior / 12m retrasado",
           case s.empresa when 'MRV' then 1 when 'Cury' then 2 when 'Tenda' then 3
                when 'Direcional' then 4 when 'Pacaembu' then 5 else 6 end as ordem
    from edicoes e join serie s on s.k = e.k
    """,
        ),
        Quadro(
            pagina=2,
            secao="3. Balanços das Empresas",
            titulo="Totais das empresas levantadas (variação %)",
            colunas=["indicador", "vs. trim. anterior", "vs. mesmo trim. ano ant.",
                     "12m atual / 12m anterior", "12m anterior / 12m retrasado"],
            sql=f"""
    with {EDICOES},
    soma as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               sum(lancamentos::numeric) as lv, sum(vendas::numeric) as vv
        from {MART}.gold_continuo_balancos_empresas group by 1
    ),
    tot as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               {num('var_lancamentos_totais_tri_anterior')}           as lt,
               {num('var_lancamentos_totais_mesmo_tri_ano_anterior')} as la,
               {num('var_vendas_totais_tri_anterior')}                as vt,
               {num('var_vendas_totais_mesmo_tri_ano_anterior')}      as va
        from {MART}.gold_continuo_balancos_empresas_totais
    )
    select e.edicao, 'Total lançamentos' as indicador,
           round(t.lt * 100, 0) as "vs. trim. anterior",
           round(t.la * 100, 0) as "vs. mesmo trim. ano ant.",
           round((( select sum(lv) from soma x where x.k between e.k - 3 and e.k)
                / nullif((select sum(lv) from soma x where x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0) as "12m atual / 12m anterior",
           round((( select sum(lv) from soma x where x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(lv) from soma x where x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0) as "12m anterior / 12m retrasado",
           1 as ordem
    from edicoes e join tot t on t.k = e.k
    union all
    select e.edicao, 'Total vendas',
           round(t.vt * 100, 0), round(t.va * 100, 0),
           round((( select sum(vv) from soma x where x.k between e.k - 3 and e.k)
                / nullif((select sum(vv) from soma x where x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0),
           round((( select sum(vv) from soma x where x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(vv) from soma x where x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0),
           2
    from edicoes e join tot t on t.k = e.k
    """,
        ),
        Quadro(
            pagina=2,
            secao="3. Balanços das Empresas",
            titulo="Financiamentos Imobiliários (BACEN)",
            colunas=["periodo", "PF Concessões (R$ mi)", "PF Taxa de Juros (%a.a)",
                     "PF Inadimplência (%)", "PJ Concessões (R$ mi)",
                     "PJ Taxa de Juros (%a.a)", "PJ Inadimplência (%)"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data)::int * 12 + extract(month from data)::int) as m,
               concessoes_pf_rs_mi pf, taxa_juros_pf_aa tpf, inadimplencia_pf_pct ipf,
               concessoes_pj_rs_mi pj, taxa_juros_pj_aa tpj, inadimplencia_pj_pct ipj
        from {MART}.gold_continuo_financiamentos_imobiliarios_pf_pj
    ),
    ref as (select edicao, k, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round(x.pf::numeric, 0) as "PF Concessões (R$ mi)",
           round(x.tpf::numeric, 1) as "PF Taxa de Juros (%a.a)",
           round(x.ipf::numeric, 1) as "PF Inadimplência (%)",
           round(x.pj::numeric, 0) as "PJ Concessões (R$ mi)",
           round(x.tpj::numeric, 1) as "PJ Taxa de Juros (%a.a)",
           round(x.ipj::numeric, 1) as "PJ Inadimplência (%)",
           x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0
        union all
        select 'Mês anterior', 2, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0 - 1
        union all
        select 'Mesmo mês do ano anterior', 3, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0 - 12
        union all
        select '12 meses até a referência', 4, sum(pf), null, null, sum(pj), null, null
        from mes where m between r.m0 - 11 and r.m0
        union all
        select '12 meses anteriores', 5, sum(pf), null, null, sum(pj), null, null
        from mes where m between r.m0 - 23 and r.m0 - 12
    ) x
    """,
        ),
        Quadro(
            pagina=2,
            secao="3. Balanços das Empresas",
            titulo="Financiamentos Habitacionais (UH)",
            colunas=["periodo", "FGTS-PJ", "SBPE Const."],
            sql=f"""
    with {EDICOES},
    serie as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               financ_hab_fgts_pj pj, financ_hab_sbpe_constr sb,
               financ_hab_fgts_pj_acumulado_12_meses pj12,
               financ_hab_sbpe_constr_acumulado_12_meses sb12
        from {MART}.gold_continuo_financiamentos_habitacionais
    )
    select e.edicao, x.rotulo as periodo, x.pj as "FGTS-PJ", x.sb as "SBPE Const.", x.ordem
    from edicoes e
    cross join lateral (
        select 'Trimestre selecionado' as rotulo, 1 as ordem, pj, sb from serie where k = e.k
        union all select 'Trimestre anterior', 2, pj, sb from serie where k = e.k - 1
        union all select 'Mesmo trim. do ano anterior', 3, pj, sb from serie where k = e.k - 4
        union all select '12 meses até a referência', 4, pj12, sb12 from serie where k = e.k
        union all select '12 meses anteriores', 5, pj12, sb12 from serie where k = e.k - 4
    ) x
    """,
        ),
    ]


def pagina_03() -> list[Quadro]:
    """Página 3 do boletim."""
    return [
        Quadro(
            pagina=3,
            secao="4. Empregos",
            titulo="Empregos Construção (CAGED)",
            colunas=["periodo", "Criação Líquida (Saldo)", "Total de Postos (Estoque)"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               total_construcao_saldo saldo, total_construcao_estoque estoque
        from {MART}.gold_continuo_empregos_caged
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           x.saldo as "Criação Líquida (Saldo)", x.estoque as "Total de Postos (Estoque)", x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, saldo, estoque from mes where m = r.m0
        union all select 'Mês anterior', 2, saldo, estoque from mes where m = r.m0 - 1
        union all select 'Mesmo mês do ano anterior', 3, saldo, estoque from mes where m = r.m0 - 12
        union all select 'Acumulado no trimestre', 4, sum(saldo), null from mes where m between r.m0 - 2 and r.m0
        union all select 'Acum. no trim. do ano anterior', 5, sum(saldo), null from mes where m between r.m0 - 14 and r.m0 - 12
    ) x
    """,
        ),
        Quadro(
            pagina=3,
            secao="4. Empregos",
            titulo="PNAD Contínua — Ocupados e Rendimento Médio Real",
            colunas=["periodo", "Ocupados Construção (mil)", "Ocupados Total (mil)",
                     "Rendimento Construção (R$)", "Rendimento Total (R$)"],
            sql=f"""
    with {EDICOES},
    base as (
        select o.periodo,
               (left(o.periodo, 4)::int * 12 + right(o.periodo, 2)::int) as m,
               o.periodo_nome, o.ocupados_construcao_mil oc, o.ocupados_total_mil ot,
               r.rendimento_construcao_rs rc, r.rendimento_total_rs rt
        from {MART}.gold_continuo_pnad_ocupados o
        join {MART}.gold_continuo_pnad_rendimento r on r.periodo = o.periodo
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, b.periodo_nome as periodo,
           b.oc as "Ocupados Construção (mil)", b.ot as "Ocupados Total (mil)",
           b.rc as "Rendimento Construção (R$)", b.rt as "Rendimento Total (R$)",
           case b.m when r.m0 then 1 when r.m0 - 3 then 2 when r.m0 - 12 then 3 end as ordem
    from ref r join base b on b.m in (r.m0, r.m0 - 3, r.m0 - 12)
    """,
        ),
        Quadro(
            pagina=3,
            secao="5. Produção Física Industrial e Vendas da Construção",
            titulo="Produção Industrial e Volume de Vendas (variação %)",
            colunas=["indicador", "PROD mesmo mês ano ant.", "PROD mês anterior",
                     "PROD mês de referência", "VENDAS mesmo mês ano ant.",
                     "VENDAS mês anterior", "VENDAS mês de referência"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               pim_pf_var_mes pm, pim_pf_var_acum_ano pa, pim_pf_var_12_meses pd,
               pmc_var_mes vm, pmc_var_acum_ano va, pmc_var_12_meses vd
        from {MART}.gold_continuo_producao_fisica
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'Variação percentual mensal' as indicador,
           (select pm from mes where m = r.m0 - 12) as "PROD mesmo mês ano ant.",
           (select pm from mes where m = r.m0 - 1)  as "PROD mês anterior",
           (select pm from mes where m = r.m0)      as "PROD mês de referência",
           (select vm from mes where m = r.m0 - 12) as "VENDAS mesmo mês ano ant.",
           (select vm from mes where m = r.m0 - 1)  as "VENDAS mês anterior",
           (select vm from mes where m = r.m0)      as "VENDAS mês de referência",
           1 as ordem
    from ref r
    union all
    select r.edicao, 'Variação percentual acumulada no ano',
           (select pa from mes where m = r.m0 - 12), (select pa from mes where m = r.m0 - 1),
           (select pa from mes where m = r.m0), (select va from mes where m = r.m0 - 12),
           (select va from mes where m = r.m0 - 1), (select va from mes where m = r.m0), 2
    from ref r
    union all
    select r.edicao, 'Variação percentual acumulada nos últimos 12 meses',
           (select pd from mes where m = r.m0 - 12), (select pd from mes where m = r.m0 - 1),
           (select pd from mes where m = r.m0), (select vd from mes where m = r.m0 - 12),
           (select vd from mes where m = r.m0 - 1), (select vd from mes where m = r.m0), 3
    from ref r
    """,
        ),
        Quadro(
            pagina=3,
            secao="6. Crédito",
            titulo="Novos Financiamentos Imobiliários por Banco (acum. no ano)",
            colunas=["banco", "UH acum. ano", "R$ bi acum. ano", "% UH", "fonte"],
            sql=f"""
    with {EDICOES},
    ref as (select edicao, ano_ed, tri_ed * 3 as mes_ed from edicoes)
    select r.edicao, g.instituicao as banco,
           g.uh_acumulado_ano as "UH acum. ano",
           round((g.volume_acumulado_ano_milhoes / 1000)::numeric, 1) as "R$ bi acum. ano",
           round((g.uh_participacao * 100)::numeric, 1) as "% UH",
           g.fonte as "fonte",
           case when g.instituicao = 'TOTAL' then 0 else 1 end as ordem_grupo,
           coalesce(g.uh_acumulado_ano, 0) as ordem
    from ref r
    join {MART}.gold_continuo_financiamentos_instituicao g
      on g.ano = r.ano_ed and g.mes = r.mes_ed
    """,
            ordenar="ordem_grupo, ordem desc",
            nota="Tabela única: ABECIP automatizada onde existe, planilha manual no histórico.",
        ),
    ]


def pagina_04() -> list[Quadro]:
    """Página 4 do boletim."""
    return [
        Quadro(
            pagina=4,
            secao="6. Crédito",
            titulo="Crédito Imobiliário / PIB (%)",
            colunas=["periodo", "Crédito Imobiliário / PIB"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data)::int * 12 + extract(month from data)::int) as m,
               to_char(data, 'MM/YY') as rotulo, credito_imobiliario_pib_pct pct
        from {MART}.gold_continuo_credito_pib
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round(x.pct::numeric, 2) as "Crédito Imobiliário / PIB", x.m as ordem
    from ref r join mes x on x.m between r.m0 - 15 and r.m0
    """,
            nota="O boletim mostra 16 meses encerrados no mês de referência.",
        ),
        Quadro(
            pagina=4,
            secao="6. Crédito",
            titulo="Nº UH por Condição de Uso",
            colunas=["fonte", "Trim. ano anterior — UH Usadas", "Trim. ano anterior — UH Novas",
                     "Trim. selecionado — UH Usadas", "Trim. selecionado — UH Novas",
                     "Trim. selecionado — UH Total"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               fgts_pf_uh_usados fu, fgts_pf_uh_novos fn,
               abecip_sbpe_fin_uh_aq_usados su, abecip_sbpe_fin_uh_aq_novos sn,
               abecip_sbpe_fin_uh_aq_total st
        from {MART}.gold_continuo_uh_condicao_uso
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'FGTS - PF' as fonte,
           (select sum(fu) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior — UH Usadas",
           (select sum(fn) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior — UH Novas",
           (select sum(fu) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado — UH Usadas",
           (select sum(fn) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado — UH Novas",
           null::numeric as "Trim. selecionado — UH Total",
           1 as ordem
    from ref r
    union all
    select r.edicao, 'SBPE (Aquisição)',
           (select sum(su) from mes where m between r.m0 - 14 and r.m0 - 12),
           (select sum(sn) from mes where m between r.m0 - 14 and r.m0 - 12),
           (select sum(su) from mes where m between r.m0 - 2 and r.m0),
           (select sum(sn) from mes where m between r.m0 - 2 and r.m0),
           (select sum(st) from mes where m between r.m0 - 2 and r.m0), 2
    from ref r
    """,
        ),
    ]


def pagina_05() -> list[Quadro]:
    """Página 5 do boletim."""
    return [
        Quadro(
            pagina=5,
            secao="7. SBPE Construção",
            titulo="SBPE Construção — unidades e valor (acum. no trimestre)",
            colunas=["indicador", "Trim. ano anterior", "Trim. selecionado", "Variação %"],
            sql=f"""
with {EDICOES},
mes as (
    select (ano * 12 + mes) as m, unidades_construcao u, valor_construcao_milhoes v
    from {SILVER}.silver_continuo_abecip_financiamentos
),
ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
select r.edicao, 'Unidades' as indicador,
       (select sum(u) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior",
       (select sum(u) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado",
       round(((select sum(u) from mes where m between r.m0 - 2 and r.m0)
            / nullif((select sum(u) from mes where m between r.m0 - 14 and r.m0 - 12), 0) - 1) * 100, 0) as "Variação %",
       1 as ordem
from ref r
union all
select r.edicao, 'Valor (R$ bilhões)',
       round(((select sum(v) from mes where m between r.m0 - 14 and r.m0 - 12) / 1000)::numeric, 2),
       round(((select sum(v) from mes where m between r.m0 - 2 and r.m0) / 1000)::numeric, 2),
       round(((select sum(v) from mes where m between r.m0 - 2 and r.m0)
            / nullif((select sum(v) from mes where m between r.m0 - 14 and r.m0 - 12), 0) - 1) * 100, 0),
       2
from ref r
""",
            nota="Fonte ABECIP automatizada. Conferido vs boletim 1T26: 47.609 un, "
                 "R$ 11,22 bi, +149% e +83% — os quatro exatos.",
        ),
        Quadro(
            pagina=5,
            secao="7. Poupança",
            titulo="Saldo Caderneta de Poupança — Captação Líquida (R$ bi)",
            colunas=["periodo", "Cap. Líq. (Bi)"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               captacao_liquida_valor v
        from {MART}.gold_continuo_saldo_poupanca
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round((x.v / 1000)::numeric, 1) as "Cap. Líq. (Bi)", x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, v from mes where m = r.m0
        union all select 'Mês anterior', 2, v from mes where m = r.m0 - 1
        union all select 'Mesmo mês do ano anterior', 3, v from mes where m = r.m0 - 12
        union all select '12 meses até a referência', 4, sum(v) from mes where m between r.m0 - 11 and r.m0
        union all select '12 meses anteriores', 5, sum(v) from mes where m between r.m0 - 23 and r.m0 - 12
    ) x
    """,
        ),
        Quadro(
            pagina=5,
            secao="7. Financiamento PF",
            titulo="Financiamento PF MCMV por faixa",
            colunas=["faixa", "Trim. ano anterior — Nº UH", "Trim. ano anterior — FIN (Bi R$)",
                     "Trim. selecionado — Nº UH", "Trim. selecionado — FIN (Bi R$)"],
            sql=f"""
    with {EDICOES},
    mes as (
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               financiamento_pf_uh_faixa_1 u1, financiamento_pf_valor_faixa_1 v1,
               financiamento_pf_uh_faixa_2 u2, financiamento_pf_valor_faixa_2 v2,
               financiamento_pf_uh_faixa_3 u3, financiamento_pf_valor_faixa_3 v3,
               financiamento_pf_uh_classe_media uc, financiamento_pf_valor_classe_media vc,
               financiamento_pf_uh_total ut, financiamento_pf_valor_total vt
        from {MART}.gold_continuo_financiamento_pf_faixa
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes),
    f as (
        select 'Faixa 1' as faixa, 1 as ordem, 'u1' as cu, 'v1' as cv union all
        select 'Faixa 2', 2, 'u2', 'v2' union all
        select 'Faixa 3', 3, 'u3', 'v3' union all
        select 'Faixa Classe Média', 4, 'uc', 'vc' union all
        select 'TOTAL', 9, 'ut', 'vt'
    )
    select r.edicao, x.faixa,
           x.ua as "Trim. ano anterior — Nº UH",
           round((x.va / 1e9)::numeric, 2) as "Trim. ano anterior — FIN (Bi R$)",
           x.ub as "Trim. selecionado — Nº UH",
           round((x.vb / 1e9)::numeric, 2) as "Trim. selecionado — FIN (Bi R$)",
           x.ordem
    from ref r
    cross join lateral (
        select 'Faixa 1' as faixa, 1 as ordem,
               (select sum(u1) from mes where m between r.m0 - 14 and r.m0 - 12) ua,
               (select sum(v1) from mes where m between r.m0 - 14 and r.m0 - 12) va,
               (select sum(u1) from mes where m between r.m0 - 2 and r.m0) ub,
               (select sum(v1) from mes where m between r.m0 - 2 and r.m0) vb
        union all
        select 'Faixa 2', 2,
               (select sum(u2) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(v2) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(u2) from mes where m between r.m0 - 2 and r.m0),
               (select sum(v2) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'Faixa 3', 3,
               (select sum(u3) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(v3) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(u3) from mes where m between r.m0 - 2 and r.m0),
               (select sum(v3) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'Faixa Classe Média', 4,
               (select sum(uc) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(vc) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(uc) from mes where m between r.m0 - 2 and r.m0),
               (select sum(vc) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'TOTAL', 9,
               (select sum(ut) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(vt) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(ut) from mes where m between r.m0 - 2 and r.m0),
               (select sum(vt) from mes where m between r.m0 - 2 and r.m0)
    ) x
    """,
        ),
    ]


def pagina_06() -> list[Quadro]:
    """Página 6 do boletim."""
    return [
        Quadro(
            pagina=6,
            secao="7. Preços",
            titulo="SINAPI (Brasil) e INCC-M",
            colunas=["indicador", "SINAPI", "INCC-M"],
            sql=f"""
    with {EDICOES},
    sin as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               custo_medio_m2 ix, var_mes vm, var_acum_ano va, var_12_meses vd
        from {MART}.gold_continuo_sinapi
    ),
    inc as (
        select (extract(year from mes)::int * 12 + extract(month from mes)::int) as m,
               indice ix, var_mes vm, var_fonte_no_ano va, var_fonte_12_meses vd
        from {MART}.gold_continuo_incc_m
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'Número índice / Custo (R$/m²)' as indicador,
           round((select ix from sin where m = r.m0)::numeric, 1) as "SINAPI",
           round((select ix from inc where m = r.m0)::numeric, 2) as "INCC-M", 1 as ordem
    from ref r
    union all
    select r.edicao, 'Variação mensal (%)',
           round((select vm from sin where m = r.m0)::numeric, 2),
           round((select vm from inc where m = r.m0)::numeric, 2), 2
    from ref r
    union all
    select r.edicao, 'Acumulado no ano (%)',
           round((select va from sin where m = r.m0)::numeric, 2),
           round((select va from inc where m = r.m0)::numeric, 2), 3
    from ref r
    union all
    select r.edicao, 'Acumulado em 12 meses (%)',
           round((select vd from sin where m = r.m0)::numeric, 2),
           round((select vd from inc where m = r.m0)::numeric, 2), 4
    from ref r
    """,
            nota="INCC-M: as colunas de acumulado da fonte estão trocadas — ver relatório.",
        ),
        Quadro(
            pagina=6,
            secao="7. Preços",
            titulo="Ticket médio das unidades lançadas vs. INCC",
            colunas=["periodo", "INCC trimestral", "MRV trimestral", "Direcional trimestral",
                     "Tenda trimestral", "INCC acum. 4T20", "MRV acum. 4T20",
                     "Direcional acum. 4T20", "Tenda acum. 4T20"],
            sql=f"""
    with {EDICOES},
    serie as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k, periodo,
               incc_var_tri_ant it, incc_var_acum_4t2020 ia,
               ticket_medio_lancamentos_mrv_var_tri_ant mt, ticket_medio_lancamentos_mrv_var_acum_4t2020 ma,
               ticket_medio_lancamentos_direcional_var_tri_ant dt, ticket_medio_lancamentos_direcional_var_acum_4t2020 da,
               ticket_medio_lancamentos_tenda_var_tri_ant tt, ticket_medio_lancamentos_tenda_var_acum_4t2020 ta
        from {MART}.gold_continuo_ticket_medio
    )
    select e.edicao, s.periodo,
           round(s.it::numeric * 100, 1) as "INCC trimestral",
           round(s.mt::numeric * 100, 1) as "MRV trimestral",
           round(s.dt::numeric * 100, 1) as "Direcional trimestral",
           round(s.tt::numeric * 100, 1) as "Tenda trimestral",
           round(s.ia::numeric * 100, 1) as "INCC acum. 4T20",
           round(s.ma::numeric * 100, 1) as "MRV acum. 4T20",
           round(s.da::numeric * 100, 1) as "Direcional acum. 4T20",
           round(s.ta::numeric * 100, 1) as "Tenda acum. 4T20",
           s.k as ordem
    from edicoes e join serie s on s.k between e.k - 8 and e.k
    """,
            nota="O boletim mostra 9 trimestres encerrados na edição selecionada.",
        ),
    ]


def pagina_07() -> list[Quadro]:
    """Página 7 do boletim."""
    return [
        Quadro(
            pagina=7,
            secao="8. Índices da Construção",
            titulo="Índices da Construção (variação %)",
            colunas=["indicador", "Índice IMOB", "Índice ABRAMAT", "Índice FipeZap", "Índice ICST"],
            sql=f"""
    with {EDICOES},
    ref as (select edicao, ano_ed, tri_ed, ano_ed * 12 + tri_ed * 3 as m0 from edicoes),
    imob as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               indice_imob_var_mes a, indice_imob_var_mes_vs_mes_ano_ant b, indice_imob_var_acum_ano c
        from {MART}.gold_continuo_indice_imob
    ),
    fipe as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               indice_fipezap_locacao_var_mes a, indice_fipezap_locacao_var_mes_vs_mes_ano_ant b,
               indice_fipezap_locacao_acum_ano c
        from {MART}.gold_continuo_fipezap
    ),
    abramat as (
        select (ano::int * 12 + mes::int) as m,
               indice_abramat_var_mes a, indice_abramat_var_mes_vs_mes_ano_ant b,
               indice_abramat_var_acum_ano c
        from manual_conjuntura.dados_mensais
    ),
    icst as (
        select (right(periodo, 4)::int * 12 + left(periodo, 2)::int) as m,
               indice_icst_var_mes_com_ajuste a, indice_icst_var_mes_vs_mes_ano_ant_com_ajuste b,
               icst_com_ajuste_sazonal ix
        from {MART}.gold_continuo_icst
    )
    select r.edicao, 'Mês de ref. vs. mês anterior' as indicador,
           round((select a from imob where m = r.m0)::numeric * 100, 1) as "Índice IMOB",
           round((select a from abramat where m = r.m0)::numeric * 100, 1) as "Índice ABRAMAT",
           round((select a from fipe where m = r.m0)::numeric * 100, 1) as "Índice FipeZap",
           round((select a from icst where m = r.m0)::numeric * 100, 1) as "Índice ICST", 1 as ordem
    from ref r
    union all
    select r.edicao, 'Mês de ref. vs. mesmo mês do ano ant.',
           round((select b from imob where m = r.m0)::numeric * 100, 1),
           round((select b from abramat where m = r.m0)::numeric * 100, 1),
           round((select b from fipe where m = r.m0)::numeric * 100, 1),
           round((select b from icst where m = r.m0)::numeric * 100, 1), 2
    from ref r
    union all
    select r.edicao, 'Acumulado no ano',
           round((select c from imob where m = r.m0)::numeric * 100, 1),
           round((select c from abramat where m = r.m0)::numeric * 100, 1),
           round((select c from fipe where m = r.m0)::numeric * 100, 1),
           round(((select ix from icst where m = r.m0)
                  / nullif((select ix from icst where m = r.ano_ed * 12), 0) - 1)::numeric * 100, 1), 3
    from ref r
    union all
    select r.edicao, 'Acumulado no ano anterior',
           round((select c from imob where m = r.m0 - 12)::numeric * 100, 1),
           round((select c from abramat where m = r.m0 - 12)::numeric * 100, 1),
           round((select c from fipe where m = r.m0 - 12)::numeric * 100, 1),
           round(((select ix from icst where m = r.m0 - 12)
                  / nullif((select ix from icst where m = (r.ano_ed - 1) * 12), 0) - 1)::numeric * 100, 1), 4
    from ref r
    """,
        ),
    ]

#: Cada página do boletim é uma função. Para incluir ou tirar um quadro,
#: mexe-se só na função da página correspondente — nada mais no arquivo
#: precisa mudar, e o construtor monta as abas a partir daqui.
PAGINAS = {
    1: pagina_01,
    2: pagina_02,
    3: pagina_03,
    4: pagina_04,
    5: pagina_05,
    6: pagina_06,
    7: pagina_07,
}


def quadros() -> list[Quadro]:
    """Todos os quadros, na ordem das páginas."""
    return [q for pg in sorted(PAGINAS) for q in PAGINAS[pg]()]


#: mantido para quem já importa a lista direto
QUADROS: list[Quadro] = quadros()

#: Quadros que o boletim publica e que não temos como reproduzir.
#: `pendente=True` marca o que está a caminho — dado identificado, ingestão
#: combinada — para não se confundir com o que não tem fonte nenhuma.
SEM_FONTE = [
    (3, "6. Crédito", "Novos Financiamentos por Banco — competências recentes",
     "PENDENTE, não ausente: os boletins mensais da ABECIP têm a tabela completa "
     "(inclusive BRB), em URL pública e previsível. A ingestão das competências "
     "2025-10 em diante foi combinada com o time que opera o OCR. Até lá o quadro "
     "abaixo mostra só o histórico da planilha manual, que termina em 09/2025."),
    (6, "6. OGU", "OGU e Desembolsos de Obras",
     "O boletim congela o SIAFI na data da edição (‘Dados de 02/01/26’); nossa "
     "extração é sempre a posição corrente, então nenhuma célula reproduz."),
]


def sql_do_quadro(q: Quadro) -> str:
    """SQL final do dataset: `edicao` (para o filtro) + colunas impressas.

    A chave de ordenação fica fora da projeção — ordenar por ela no chart
    exigiria projetá-la, e aí ela viraria uma coluna visível que o boletim
    não tem.
    """
    colunas = ", ".join(['"edicao"'] + [f'"{c}"' for c in q.colunas])
    return f"select {colunas}\nfrom (\n{q.sql}\n) q\norder by edicao, {q.ordenar}"


SCHEMA = "conjuntura_continuo_mart"
DATABASE_NAME = "Cidades"
SLUG = "boletim-conjuntura"
TITULO = "Boletim de Conjuntura — Trimestral"
PREFIXO = "Boletim | "
TABS_ID = "TABS-BOLETIM"
FILTRO_ID = "NATIVE_FILTER-edicao"


class Superset:
    """Sessão autenticada por FORMULÁRIO.

    O bearer JWT desta instância é cego para os dashboards da conjuntura —
    `GET /api/v1/dashboard/<slug>` devolve 404 com bearer e 200 com cookie.
    """

    def __init__(self, dry_run: bool = False) -> None:
        self.base = os.environ["SUPERSET_URL"].rstrip("/")
        self.dry_run = dry_run
        self.s = requests.Session()
        pagina = self.s.get(f"{self.base}/login/", timeout=30)
        pagina.raise_for_status()
        achado = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', pagina.text)
        self.s.post(
            f"{self.base}/login/",
            data={
                "username": os.environ["SUPERSET_USERNAME"],
                "password": os.environ["SUPERSET_PASSWORD"],
                "csrf_token": achado.group(1) if achado else "",
            },
            timeout=30,
        ).raise_for_status()
        csrf = self.s.get(f"{self.base}/api/v1/security/csrf_token/", timeout=30)
        csrf.raise_for_status()
        self.h = {
            "X-CSRFToken": csrf.json()["result"],
            "Referer": self.base,
            "Content-Type": "application/json",
        }

    def listar(self, recurso: str) -> list[dict]:
        pg, out = 0, []
        while True:
            corpo = self.s.get(
                f"{self.base}/api/v1/{recurso}/",
                params={"q": f"(page:{pg},page_size:100)"},
                timeout=60,
            ).json()
            out += corpo["result"]
            if len(corpo["result"]) < 100:
                return out
            pg += 1

    def get(self, recurso: str, ident) -> dict | None:
        r = self.s.get(f"{self.base}/api/v1/{recurso}/{ident}", timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["result"]

    def criar(self, recurso: str, payload: dict) -> int:
        if self.dry_run:
            print(f"  [dry-run] criaria {recurso}: {payload.get('table_name') or payload.get('slice_name') or payload.get('dashboard_title')}")
            return -1
        r = self.s.post(f"{self.base}/api/v1/{recurso}/", headers=self.h, json=payload, timeout=90)
        if not r.ok:
            raise RuntimeError(f"{recurso} recusado (HTTP {r.status_code}): {r.text[:300]}")
        return r.json()["id"]

    def atualizar(self, recurso: str, ident, payload: dict) -> None:
        if self.dry_run:
            print(f"  [dry-run] atualizaria {recurso} {ident}")
            return
        r = self.s.put(f"{self.base}/api/v1/{recurso}/{ident}", headers=self.h, json=payload, timeout=90)
        if not r.ok:
            raise RuntimeError(f"{recurso} {ident} recusado (HTTP {r.status_code}): {r.text[:300]}")

    def dados(self, query_context: dict) -> tuple[bool, str]:
        r = self.s.post(f"{self.base}/api/v1/chart/data", headers=self.h, json=query_context, timeout=180)
        if not r.ok:
            return False, str(r.json().get("message"))[:120]
        return True, f"{r.json()['result'][0].get('rowcount')} linhas"


# --------------------------------------------------------------------------
# um quadro -> dataset virtual + chart


def nome_dataset(q: Quadro) -> str:
    """Tabela FÍSICA do quadro, materializada pelo dbt em `gold/boletim/`.

    Antes cada quadro virava um dataset VIRTUAL, e o SQL — que monta a série
    por edição, com janelas móveis e comparações — voltava ao Postgres a cada
    carregamento de página. Agora o processamento acontece uma vez, no `dbt
    run`, e o dashboard só lê tabela pronta.
    """
    base = unicodedata.normalize("NFKD", q.titulo).encode("ascii", "ignore").decode()
    base = re.sub(r"[^a-z0-9]+", "_", base.lower()).strip("_")[:44]
    return f"gold_boletim_p{q.pagina}_{base}"


def garantir_quadro(api: Superset, q: Quadro, cache: dict) -> int | None:
    """Cria ou atualiza o dataset virtual e o chart do quadro. Devolve o id."""
    dsn = nome_dataset(q)

    ds = cache["datasets"].get(dsn)
    if ds is None:
        # dataset físico: aponta para a tabela que o dbt materializou. Se ela
        # não existir, o erro tem que ser alto — dashboard mudo é pior.
        ds = api.criar(
            "dataset",
            {"database": cache["db"], "schema": SCHEMA, "table_name": dsn},
        )
        if ds == -1:
            return None
        cache["datasets"][dsn] = ds
    if not api.dry_run:
        api.s.put(f"{api.base}/api/v1/dataset/{ds}/refresh", headers=api.h, timeout=120)

    detalhe = api.get("dataset", ds) or {}
    colunas = [c["column_name"] for c in detalhe.get("columns", [])]
    visiveis = [c for c in q.colunas if c in colunas]

    params = {
        "datasource": f"{ds}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": visiveis,
        "groupby": [],
        "metrics": [],
        "percent_metrics": [],
        "order_by_cols": [],
        "row_limit": 1000,
        "server_page_length": 50,
        "table_timestamp_format": "smart_date",
    }
    qc = {
        "datasource": {"id": ds, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [], "extras": {"having": "", "where": ""},
                "applied_time_extras": {}, "columns": visiveis, "metrics": [],
                "orderby": [], "annotation_layers": [], "row_limit": 1000,
                "series_limit": 0, "order_desc": False, "url_params": {},
                "custom_params": {}, "custom_form_data": {},
            }
        ],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }
    corpo = {
        "slice_name": PREFIXO + q.titulo,
        "viz_type": "table",
        "datasource_id": ds,
        "datasource_type": "table",
        "params": json.dumps(params),
        "query_context": json.dumps(qc),
    }
    nome = PREFIXO + q.titulo
    cid = cache["charts"].get(nome)
    if cid:
        api.atualizar("chart", cid, corpo)
    else:
        cid = api.criar("chart", corpo)
        if cid == -1:
            return None
        cache["charts"][nome] = cid
    return cid


# --------------------------------------------------------------------------
# uma função por página


def construir_pagina(api: Superset, numero: int, cache: dict) -> list[dict]:
    """Constrói os quadros de uma página e devolve os nós de layout."""
    nos: list[dict] = []
    for q in PAGINAS[numero]():
        cid = garantir_quadro(api, q, cache)
        if cid:
            nos.append({"tipo": "CHART", "chart": cid, "titulo": PREFIXO + q.titulo})
            print(f"    {q.titulo[:52]:<54} chart {cid}")
    for pg, _secao, titulo, motivo in SEM_FONTE:
        if pg == numero:
            nos.append({"tipo": "MARKDOWN", "titulo": titulo, "motivo": motivo})
            print(f"    [aviso] {titulo[:52]}")
    return nos




#: as páginas construíveis saem do próprio spec — acrescentar uma
#: `pagina_08()` acima já a torna construível, sem tocar aqui.
CONSTRUTORES = {n: (lambda api, cache, n=n: construir_pagina(api, n, cache))
                for n in sorted(PAGINAS)}


# --------------------------------------------------------------------------
# layout


def nos_da_pagina_publicada(layout: dict, numero: int) -> list[dict]:
    """Lê do dashboard já publicado os nós de uma página, para preservá-la."""
    tab = f"TAB-P{numero}"
    if tab not in layout:
        return []
    nos = []
    for row in layout[tab].get("children", []):
        filho = layout.get(layout[row]["children"][0], {})
        if filho.get("type") == "CHART":
            nos.append({"tipo": "CHART", "chart": filho["meta"]["chartId"],
                        "titulo": filho["meta"].get("sliceName", "")})
        elif filho.get("type") == "MARKDOWN":
            nos.append({"tipo": "MARKDOWN_BRUTO", "code": filho["meta"]["code"]})
    return nos


def monta_layout(por_pagina: dict[int, list[dict]]) -> str:
    layout = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [TABS_ID], "parents": [], "meta": {}},
        TABS_ID: {"id": TABS_ID, "type": "TABS", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    for pg in sorted(por_pagina):
        tab, rotulo = f"TAB-P{pg}", f"Página {pg}"
        layout[TABS_ID]["children"].append(tab)
        layout[tab] = {"id": tab, "type": "TAB", "children": [], "parents": ["ROOT_ID", TABS_ID],
                       "meta": {"text": rotulo, "defaultText": rotulo, "placeholder": rotulo}}
        for i, no in enumerate(por_pagina[pg]):
            row = f"ROW-P{pg}-{i:02d}"
            layout[tab]["children"].append(row)
            if no["tipo"] == "CHART":
                nid = f"CHART-{no['chart']}"
                meta = {"chartId": no["chart"], "width": 12, "height": 50,
                        "sliceName": no["titulo"], "index": f"{i:03d}"}
                tipo = "CHART"
            else:
                nid = f"MARKDOWN-P{pg}-{i:02d}"
                code = no.get("code") or f"### ⚠️ {no['titulo']}\n\n{no['motivo']}"
                meta = {"width": 12, "height": 20, "code": code}
                tipo = "MARKDOWN"
            layout[row] = {"id": row, "type": "ROW", "children": [nid],
                           "parents": ["ROOT_ID", TABS_ID, tab],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout[nid] = {"id": nid, "type": tipo, "children": [],
                           "parents": ["ROOT_ID", TABS_ID, tab, row], "meta": meta}
    return json.dumps(layout)


def filtro_de_edicao(dataset_id: int) -> dict:
    """Filtro de trimestre. Sem edição padrão, de propósito.

    Qual trimestre olhar é escolha de quem consulta, e o dbt já publica todas
    as edições. Cravar uma padrão no build significaria que, ao entrar o
    trimestre seguinte, o dashboard continuaria abrindo no anterior até alguém
    editar Python e republicar — decisão sobre trimestre não mora em código.
    """
    return {
        "id": FILTRO_ID, "name": "Trimestre", "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "targets": [{"column": {"name": "edicao"}, "datasetId": dataset_id}],
        "defaultDataMask": {},
        "controlValues": {"multiSelect": False, "enableEmptyFilter": True,
                          "defaultToFirstItem": False, "searchAllOptions": False,
                          "inverseSelection": False},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "cascadeParentIds": [], "description": "Edição do boletim",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paginas", help="ex.: 3,5 — reconstrói só essas páginas")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    alvo = ([int(x) for x in args.paginas.split(",")] if args.paginas
            else sorted(CONSTRUTORES))
    api = Superset(dry_run=args.dry_run)

    cache = {
        "db": next(d["id"] for d in api.listar("database") if d["database_name"] == DATABASE_NAME),
        "datasets": {d["table_name"]: d["id"] for d in api.listar("dataset")},
        "charts": {c["slice_name"]: c["id"] for c in api.listar("chart")},
    }

    publicado = api.get("dashboard", SLUG)
    layout_atual = json.loads(publicado["position_json"]) if publicado and publicado.get("position_json") else {}

    por_pagina: dict[int, list[dict]] = {}
    for pg in sorted(CONSTRUTORES):
        if pg in alvo:
            print(f"  Página {pg}")
            por_pagina[pg] = CONSTRUTORES[pg](api, cache)
        else:
            preservado = nos_da_pagina_publicada(layout_atual, pg)
            if preservado:
                por_pagina[pg] = preservado
                print(f"  Página {pg}: preservada ({len(preservado)} blocos)")

    if api.dry_run:
        print("\n[dry-run] layout não gravado.")
        return

    ds_filtro = next((n["chart"] for p in por_pagina.values() for n in p if n["tipo"] == "CHART"), None)
    dataset_filtro = api.get("chart", ds_filtro)["datasource_id"] if ds_filtro else None
    payload = {
        "dashboard_title": TITULO, "slug": SLUG, "published": True,
        "position_json": monta_layout(por_pagina),
    }
    if dataset_filtro:
        payload["json_metadata"] = json.dumps(
            {"native_filter_configuration": [filtro_de_edicao(dataset_filtro)],
             "cross_filters_enabled": False}
        )

    if publicado:
        api.atualizar("dashboard", publicado["id"], payload)
        did = publicado["id"]
        print(f"\ndashboard atualizado: {did}")
    else:
        did = api.criar("dashboard", payload)
        print(f"\ndashboard criado: {did}")

    for nos in por_pagina.values():
        for no in nos:
            if no["tipo"] != "CHART":
                continue
            chart = api.get("chart", no["chart"]) or {}
            atuais = {d["id"] for d in (chart.get("dashboards") or [])}
            if did not in atuais:
                api.atualizar("chart", no["chart"], {"dashboards": sorted(atuais | {did})})

    ligados = api.s.get(f"{api.base}/api/v1/dashboard/{SLUG}/charts", timeout=30).json()["result"]
    print(f"charts ligados: {len(ligados)}")
    print(f"URL: {api.base}/superset/dashboard/{SLUG}/")


if __name__ == "__main__":
    main()
