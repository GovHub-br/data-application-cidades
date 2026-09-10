{#
    Acumulado-no-ano (YTD) de UH e desembolso por empreendimento, braço SNH.
    Change: consolidar-schemas-historico-reloginho (D3) — antes era o modelo de fluxo YTD autônomo (grão idêntico ao das
    silvers de frente: (frente_mcmv, apf, dt_referencia); 99,994% de casamento
    de chave). Agora incorporado por LEFT JOIN no fim de historico_silver_tail().

    Cobertura: a família `quantidade_de_uhs_*_do_ano_de_referencia` só existe de
    2024-06 em diante e NÃO retroage — as 9 colunas ficam NULL em ~95% das
    linhas históricas POR CONSTRUÇÃO DA FONTE, não por lacuna de pipeline.

    Dedup igual à prata_dhist_snh_apf_mes: (agente_financeiro, apf,
    dt_referencia), desempate por prioridade_reentrega. Um `qualify` final
    garante 1 linha por (frente_mcmv, apf, dt_referencia) — no-op nos dados
    atuais (cada APF tem 1 agente/mês), defensivo contra drift.
#}
{% macro historico_fluxo_ano_arm() %}
{%- set snh_familias = familias_snh_empreendimento() -%}
    with
    fluxo_tipado as (
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
            {{ parse_hist_numeric('valor_desembolsado_do_ano_de_referencia') }}
            as valor_desembolsado_ano,
            prioridade_reentrega,
            data_de_movimento,
            source_file
        from {{ ref(f.modelo) }}
        where nullif(trim(apf::text), '') is not null
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    fluxo_dedup as (
        select
            *,
            row_number() over (
                partition by agente_financeiro, apf, dt_referencia
                order by
                    prioridade_reentrega desc,
                    try_cast(nullif(trim(data_de_movimento::text), '') as date) nulls last,
                    source_file
            ) as rn
        from fluxo_tipado
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
        quantidade_uh_contratadas_ano,
        quantidade_uh_entregues_ano,
        quantidade_uh_vigentes_ano,
        quantidade_uh_distratadas_ano,
        quantidade_uh_contratadas_jan,
        quantidade_uh_entregues_jan,
        quantidade_uh_vigentes_jan,
        quantidade_uh_distratadas_jan,
        valor_desembolsado_ano
    from fluxo_dedup
    where rn = 1
    qualify row_number() over (
        partition by frente_mcmv, apf, dt_referencia order by 1
    ) = 1
{% endmacro %}
