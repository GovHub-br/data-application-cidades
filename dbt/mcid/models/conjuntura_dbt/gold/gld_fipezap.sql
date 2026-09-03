{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice FipeZap de locação — número índice e
-- variações mensais. Página 7 (seção 8).
--
-- AUTOMATIZADO em 2026-08-27 (era 100% manual). Dois bugs corrigidos na
-- ingestão pra chegar aqui (ver comentário em cliente_fipe.py):
--   1. O "número índice" (nível absoluto do índice) nunca foi extraído do
--      xlsx da FIPE — só var_mensal e var_ano. A coluna existe na mesma
--      aba (col 22, "Número-Índice"), 5 colunas antes da var_mensal já
--      usada — só faltava ler.
--   2. `LINHA_FIM_DADOS = 223` hardcoded cortava a extração em mar/2026,
--      descartando silenciosamente abr-jul/2026 (a planilha real já ia
--      até jul/2026, linha 226). Corrigido pra ir até o fim da planilha.
--
-- Número-índice validado exato contra o manual em TODOS os meses
-- conferidos (ago/2025 a mai/2026). `acum_ano` (variação desde dez do
-- ano anterior) não vem pronto do xlsx da FIPE — calculado aqui a partir
-- do número-índice (índice_atual / índice_dez_ano_anterior - 1), mesma
-- técnica usada no Índice IMOB.
--
-- ATENÇÃO (achado ao validar, não é bug): var_mes/var_ano de fev-abr/2026
-- batem exato com o manual em ago/2025-jan/2026 e mai/2026, mas destoam
-- ~0,2-0,6 p.p. em fev-abr/2026 -- a série do FipeZap sofre revisão
-- retroativa (documentado em cliente_fipe.py), e o manual foi digitado
-- de um boletim com uma vintage mais antiga do índice pra esses 3 meses.
-- O automatizado aqui reflete a vintage mais recente (mais correta) --
-- é esperado ele não bater 100% com boletim antigo pros últimos 1-2
-- trimestres, e isso se resolve sozinho conforme a série "assenta".

with fipezap as (
    select
        extract(year from data_referencia::date)::int  as ano,
        extract(month from data_referencia::date)::int as mes,
        imoveis_residenciais_locacao_numero_indice_total as indice_fipezap_numero_indice_locacao,
        imoveis_residenciais_locacao_var_mensal_total    as indice_fipezap_locacao_var_mes,
        imoveis_residenciais_locacao_var_ano_total       as indice_fipezap_locacao_var_mes_vs_mes_ano_ant
    from {{ ref('slv_fipezap_locacao') }}
),

dez_ano_anterior as (
    select
        ano + 1 as ano_seguinte,
        indice_fipezap_numero_indice_locacao as indice_dez_ano_anterior
    from fipezap
    where mes = 12
),

automatico as (
    select
        f.ano,
        f.mes,
        f.indice_fipezap_numero_indice_locacao,
        f.indice_fipezap_locacao_var_mes,
        f.indice_fipezap_locacao_var_mes_vs_mes_ano_ant,
        f.indice_fipezap_numero_indice_locacao / d.indice_dez_ano_anterior - 1
            as indice_fipezap_locacao_acum_ano
    from fipezap f
    left join dez_ano_anterior d on d.ano_seguinte = f.ano
)

select
    a.ano || '-' || lpad(a.mes::text, 2, '0') as periodo,
    a.ano,
    a.mes,
    make_date(a.ano, a.mes, 1) as data_referencia,
    a.indice_fipezap_numero_indice_locacao,
    a.indice_fipezap_locacao_var_mes,
    a.indice_fipezap_locacao_var_mes_vs_mes_ano_ant,
    a.indice_fipezap_locacao_acum_ano
from automatico a
order by ano desc, mes desc
