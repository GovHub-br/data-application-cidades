{{ config(materialized='table') }}

with posicao as (
    select *
    from (
        select
            trim(cod_empreendimento::text) as codigo_empreendimento,
            trim(dte_ano_mes::text) as competencia_posicao,
            nullif(replace(prc_obra_executada_ult::text, ',', '.'), '')::numeric as percentual_obra,
            dt_inicio::text as data_inicio_raw,
            dt_termino::text as data_termino_raw,
            dt_inauguracao::text as data_inauguracao_raw,
            row_number() over (
                partition by trim(cod_empreendimento::text)
                order by trim(dte_ano_mes::text) desc nulls last
            ) as rn
        from {{ ref('bronze_sharepoint_fgts_canal_tab_ao_1_tab_empreendimentos_posicoes') }}
    ) x where rn = 1
),
ao1 as (
    select
        trim(e.cod_empreendimento::text) as codigo_empreendimento,
        trim(e.txt_nome_empreendimento::text) as nome_empreendimento,
        trim(e.cod_municipio::text) as codigo_municipio,
        null::text as municipio,
        null::text as uf,
        trim(e.txt_localidade::text) as localidade,
        trim(e.txt_objeto::text) as objeto,
        nullif(trim(e.qtd_unidades_financiadas::text), '')::integer as quantidade_unidades,
        null::integer as quantidade_unidades_concluidas,
        null::integer as quantidade_unidades_entregues,
        p.percentual_obra,
        p.competencia_posicao,
        p.data_inicio_raw,
        p.data_termino_raw,
        p.data_inauguracao_raw,
        null::numeric as latitude,
        null::numeric as longitude,
        null::numeric as valor_investimento_pj,
        null::numeric as valor_contratado_pj,
        null::text as situacao_contrato_pj,
        null::text as entidade_pj,
        null::text as cnpj_entidade_pj,
        'FGTS_AO1'::text as fonte
    from {{ ref('bronze_sharepoint_fgts_canal_tab_ao_1_tab_empreendimentos') }} e
    left join posicao p
      on p.codigo_empreendimento = trim(e.cod_empreendimento::text)
),
pj as (
    select
        trim(nu_apf::text) as codigo_empreendimento,
        trim(no_empreendimento::text) as nome_empreendimento,
        trim(cod_municipio_ibge::text) as codigo_municipio,
        null::text as municipio,
        null::text as uf,
        trim(no_logradouro::text) as localidade,
        null::text as objeto,
        nullif(trim(qt_unidades_financiadas::text), '')::integer as quantidade_unidades,
        nullif(trim(qt_unidades_concluidas::text), '')::integer as quantidade_unidades_concluidas,
        nullif(trim(qt_unidades_entregues::text), '')::integer as quantidade_unidades_entregues,
        nullif(replace(percentual_obra_realizado::text, ',', '.'), '')::numeric as percentual_obra,
        trim(dt_movimento::text) as competencia_posicao,
        trim(dt_inicio_obra::text) as data_inicio_raw,
        trim(dt_termino_obra::text) as data_termino_raw,
        null::text as data_inauguracao_raw,
        {{ linha_financiada_numero('latitude') }} as latitude,
        {{ linha_financiada_numero('longitude') }} as longitude,
        {{ linha_financiada_numero('vi') }} as valor_investimento_pj,
        {{ linha_financiada_numero('ve') }} as valor_contratado_pj,
        trim(situacao_contrato::text) as situacao_contrato_pj,
        trim(rz_social::text) as entidade_pj,
        trim(cgc::text) as cnpj_entidade_pj,
        'GEAVO_PJ'::text as fonte
    from {{ ref('bronze_geavo_fgts_pj') }}
    where nullif(trim(nu_apf::text), '') is not null
)
select
    md5(concat_ws('|', fonte, codigo_empreendimento)) as id_empreendimento_linha_financiada,
    *,
    current_timestamp as dt_silver
from (
    select * from ao1
    union all
    select * from pj
) u
