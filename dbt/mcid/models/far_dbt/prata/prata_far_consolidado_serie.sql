{{ config(materialized="table") }}

-- Prata: situação de cada proposta em cada remessa semanal do consolidado GFAR.
-- A remessa traz uma linha por movimento da proposta; fica o movimento mais recente
-- daquela semana, o mesmo critério da prata_far_empreendimento_natureza.
with
    remessas as (
        select
            to_date(substring(filename from 'CONSOLIDADO_([0-9]{8})'), 'YYYYMMDD') as dt_remessa,
            trim(no_identificacao_proposta) as id_proposta,
            {{ var('schema_udfs') }}.normalize_apf(nu_apf) as apf,
            nullif(trim(no_uf), '') as uf,
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,
            {{ parse_int("co_fase") }} as co_fase,
            {{ parse_int("co_etapa") }} as co_etapa,
            {{ parse_int("co_status") }} as co_status,
            {{ parse_int("co_fluxo") }} as co_fluxo,
            {{ parse_int("co_tipo_de_demanda") }} as co_tipo_demanda,
            coalesce(
                {{ parse_int("nu_qt_uh_empreendimento_contratacao") }},
                {{ parse_int("nu_qt_uh_empreendimento") }}
            ) as qt_uh,
            {{ parse_financial_value("vr_empreendimento_far") }} as valor_far,
            {{ var('schema_udfs') }}.parse_date_br(dt_movimento) as dt_movimento,
            nullif(trim(hr_movimento), '') as hr_movimento
        from {{ ref("bronze_shpt_his_mcidades_consolidado_serie") }}
        where nullif(trim(no_identificacao_proposta), '') is not null
    ),

    ordenadas as (
        select
            *,
            row_number() over (
                partition by dt_remessa, id_proposta
                order by
                    dt_movimento desc nulls last,
                    hr_movimento desc nulls last,
                    co_fase desc nulls last
            ) as rn
        from remessas
    )

select
    dt_remessa,
    id_proposta,
    apf,
    uf,
    municipio,
    cod_ibge,
    co_fase,
    co_etapa,
    co_status,
    co_fluxo,
    co_tipo_demanda,
    qt_uh,
    valor_far,
    dt_movimento
from ordenadas
where rn = 1
