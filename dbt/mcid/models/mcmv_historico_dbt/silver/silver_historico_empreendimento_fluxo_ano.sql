{{ config(materialized="table", schema="mcmv_historico") }}

-- SILVER — acumulado-no-ano (YTD) de UH e desembolso por empreendimento, SNH.
-- Change: colunas-orfas-bronze-historico (Bloco B).
--
-- Grão: 1 linha por (frente_mcmv, apf, dt_referencia) mensal. SNH-only
-- (BB + CAIXA). Cobertura: 2024-10 → último snapshot (a família
-- `quantidade_de_uhs_*_do_ano_de_referencia` não retroage).
--
-- As colunas `_ano` são acumuladas DENTRO do ano-calendário e reiniciam em
-- janeiro. O fluxo mensal sai no consumidor:
--   x_ano - lag(x_ano) over (partition by frente_mcmv, apf, year(dt_referencia)
--                            order by dt_referencia)
-- As colunas `_jan` são a baseline de janeiro (para reconstruir a série YTD
-- retroativamente). `valor_desembolsado_ano` tem fill baixo (ver schema.yml).
--
-- NÃO entra no gold_snapshot / gold_marco (é fluxo, não estado). Dedup igual à
-- silver_historico_snh_apf_mes: (agente_financeiro, apf, dt_referencia),
-- desempate por prioridade_reentrega.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. As bronzes precisam existir no compile
-- (coalesce_present_parsed introspecciona a relação).
{% set snh_familias = familias_snh_empreendimento() %}

with

    tipado as (
        {% for f in snh_familias %}
        select
            upper(nullif(trim(agente_financeiro::text), '')) as agente_financeiro,
            case
                upper(nullif(trim(modalidade::text), ''))
                when 'FAR' then 'FAR'
                when 'ENTIDADES' then 'Entidades'
                when 'FDS' then 'Entidades'
                when 'FDS / ENTIDADES' then 'Entidades'
                when 'RURAL' then 'Rural'
                when 'PNHR' then 'Rural'
                else nullif(trim(modalidade::text), '')
            end as frente_mcmv,
            nullif(trim(apf::text), '') as apf,
            date_trunc('month', dt_referencia)::date as dt_referencia,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_contratadas_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_contratadas_ano,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_entregues_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_entregues_ano,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_vigentes_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_vigentes_ano,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_distratadas_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_distratadas_ano,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_contratadas_em_janeiro_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_contratadas_jan,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_entregues_em_janeiro_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_entregues_jan,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_vigentes_em_janeiro_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_vigentes_jan,
            {{ coalesce_present_parsed(ref(f.modelo), ['quantidade_de_uhs_distratadas_em_janeiro_do_ano_de_referencia'], 'parse_hist_bigint', 'bigint') }}
            as quantidade_uh_distratadas_jan,
            -- valor_desembolsado_do_ano_de_referencia (R$): presente nos dois
            -- agentes (BB ~1%, CAIXA ~75% — fill baixo, ver schema.yml).
            {{ parse_hist_numeric('valor_desembolsado_do_ano_de_referencia') }}
            as valor_desembolsado_ano,
            prioridade_reentrega,
            data_de_movimento,
            source_file,
            hash_linha
        from {{ ref(f.modelo) }}
        where nullif(trim(apf::text), '') is not null
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by agente_financeiro, apf, dt_referencia
                order by
                    prioridade_reentrega desc,
                    try_cast(nullif(trim(data_de_movimento::text), '') as date) nulls last,
                    source_file
            ) as rn
        from tipado
        where
            dt_referencia is not null
            and agente_financeiro is not null
            and frente_mcmv is not null
            and coalesce(
                quantidade_uh_contratadas_ano, quantidade_uh_entregues_ano,
                quantidade_uh_vigentes_ano, quantidade_uh_distratadas_ano,
                quantidade_uh_contratadas_jan, quantidade_uh_entregues_jan,
                quantidade_uh_vigentes_jan, quantidade_uh_distratadas_jan
            ) is not null
    )

select
    frente_mcmv,
    apf,
    dt_referencia,
    agente_financeiro,
    quantidade_uh_contratadas_ano,
    quantidade_uh_entregues_ano,
    quantidade_uh_vigentes_ano,
    quantidade_uh_distratadas_ano,
    quantidade_uh_contratadas_jan,
    quantidade_uh_entregues_jan,
    quantidade_uh_vigentes_jan,
    quantidade_uh_distratadas_jan,
    valor_desembolsado_ano,
    'snh'::text as fonte_serie,
    source_file,
    hash_linha,
    current_timestamp as dt_silver
from dedup
where rn = 1
