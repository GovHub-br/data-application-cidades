{{ config(materialized="table") }}

-- OURO do reloginho — quebra nacional/região/UF da série mensal cross-frente
-- (Classe Média, Reforma Casa Brasil, MCMV Cidades, Pró-Moradia), mesmo
-- padrão de `ouro_historico_serie_mensal_frentes_novas` em `mcmv_historico_dbt`
-- (change criar-ouro-reloginho-frentes-novas, design.md D3). Lê
-- `prata_reloginho_frentes_novas_resumo_mes_uf` (já deduplicada e agregada
-- por frente/mês/UF) — dedup por chave de negócio já feita na prata, este
-- modelo só resolve região e agrega em 3 níveis via GROUPING SETS.
--
-- Nível 'nacional' desta tabela deve bater exatamente com
-- `ouro_reloginho_indicadores_frentes_novas` (mesma fonte, mesma dedup, dois
-- caminhos independentes até o mesmo número — verificação de sanidade na
-- task 5.2).
{% set base_uf = ref('prata_reloginho_frentes_novas_resumo_mes_uf') %}
{% set regiao_uf = ref('dominio_regiao_uf') %}

with

    com_regiao as (
        select
            b.frente_mcmv,
            b.mes_referencia,
            b.uf,
            b.n_contratos,
            b.valor_contratado,
            r.regiao_sigla,
            r.regiao_nome
        from {{ base_uf }} as b
        left join {{ regiao_uf }} as r on b.uf = upper(trim(r.uf))
    ),

    agg as (
        select
            frente_mcmv,
            mes_referencia,
            case
                when grouping(uf) = 0
                then 'uf'
                when grouping(regiao_sigla) = 0
                then 'regiao'
                else 'nacional'
            end as nivel_agregacao,
            -- uf carrega a chave geográfica do PRÓPRIO nível desta linha
            -- (mesmo desenho de ouro_historico_serie_mensal_frentes_novas,
            -- necessário para (frente_mcmv, mes_referencia, nivel_agregacao,
            -- uf) ser de fato único).
            case
                when grouping(uf) = 0
                then uf
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                else 'BR'
            end as uf,
            case
                when grouping(regiao_sigla) = 0
                then regiao_sigla
                when grouping(uf) = 0
                then max(regiao_sigla)
                else 'BR'
            end as regiao_sigla,
            case
                when grouping(regiao_sigla) = 0 or grouping(uf) = 0
                then max(regiao_nome)
            end as regiao_nome,
            sum(n_contratos) as n_contratos,
            sum(valor_contratado) as valor_contratado
        from com_regiao
        group by
            grouping sets (
                (frente_mcmv, mes_referencia),
                (frente_mcmv, mes_referencia, regiao_sigla),
                (frente_mcmv, mes_referencia, uf)
            )
    )

select *
from agg
order by frente_mcmv, mes_referencia, nivel_agregacao, regiao_sigla, uf
