{{ config(materialized="table") }}

with fonte as (
    select
        *,
        row_number() over (order by dt_referencia, nu_contrato, dt_evento, vr_evento) as rn
    from {{ source("sftp_mcmv", "pmcmv_faixa3_mcid_2026_06_26") }}
)

select
    md5(concat_ws('|', 'classe-media', nu_contrato, dt_referencia, dt_evento, rn::text)) as id_silver_frente,
    'Minha Casa Minha Vida'::text as programa,
    'Classe Media'::text as frente_mcmv,
    'Financiada'::text as grupo_linha,
    'Faixa 3 / Classe Media'::text as linha_mcmv,
    'contrato_pf_fgts'::text as grao_registro,
    'sftp'::text as fonte_camada,
    'sftp'::text as fonte_schema,
    'pmcmv_faixa3_mcid_2026_06_26'::text as fonte_tabela,
    'GEAVO/FGTS/pmcmv_faixa3'::text as fonte_minio_staging,
    linha_apf::text as apf,
    nu_contrato::text as contrato,
    nu_codigo_atu::text as codigo_empreendimento,
    null::text as nome_empreendimento,
    co_municipio_ibge::text as codigo_ibge_municipio,
    no_municipio_imovel::text as municipio,
    sg_uf_imovel::text as uf,
    'Mutuario'::text as responsavel_tipo,
    null::text as responsavel_id,
    null::text as responsavel_nome,
    null::text as agente_financeiro,
    null::integer as quantidade_empreendimentos,
    1::integer as quantidade_contratos,
    1::integer as quantidade_uh,
    null::integer as quantidade_uh_entregues,
    {{ parse_financial_value('vr_investimento') }} as valor_contratado,
    null::numeric(15, 2) as valor_desembolsado,
    null::numeric(10, 2) as percentual_execucao_fisica,
    null::numeric(10, 2) as percentual_execucao_financeira,
    coalesce(situacao_garantia, modalidade, faixa_renda)::text as status_operacional,
    {{ target.schema }}.parse_date_br(dt_referencia) as dt_referencia,
    {{ target.schema }}.parse_date_br(dt_evento) as dt_contratacao,
    null::date as dt_inicio_obra,
    null::date as dt_previsao_entrega,
    null::date as dt_entrega,
    coalesce({{ target.schema }}.parse_date_br(dt_remessa), {{ target.schema }}.parse_date_br(dt_ingest), {{ target.schema }}.parse_date_br(dt_referencia)) as dt_ultima_atualizacao,
    'Dados GEAVO/FGTS; nome e documento do mutuario nao sao expostos nesta silver de dashboard.'::text as observacao_silver,
    current_timestamp as dt_silver
from fonte
where nullif(trim(nu_contrato), '') is not null
