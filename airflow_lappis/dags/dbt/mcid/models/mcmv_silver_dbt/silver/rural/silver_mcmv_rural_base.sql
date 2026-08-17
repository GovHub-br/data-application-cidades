{{ config(materialized="table") }}

with bb as (
    select
        string_to_array(dt, '|') as p
    from {{ source("sftp_mcmv", "int057_ministeriocidades_pnhr_bb_empreendimentos_20241031") }}
    where nullif(trim(dt), '') is not null
),

caixa as (
    select
        string_to_array(dt, '|') as p
    from {{ source("sftp_mcmv", "int065_ministeriocidades_pnhr_caixa_empreendimentos_20240830") }}
    where nullif(trim(dt), '') is not null
),

bb_padronizado as (
    select
        md5(concat_ws('|', 'rural-bb', p[3], p[1])) as id_silver_frente,
        'Minha Casa Minha Vida'::text as programa,
        'Rural'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'PNHR Rural BB'::text as linha_mcmv,
        'empreendimento_apf_pipe'::text as grao_registro,
        'sftp'::text as fonte_camada,
        'sftp'::text as fonte_schema,
        'int057_ministeriocidades_pnhr_bb_empreendimentos_20241031'::text as fonte_tabela,
        'GEFUS/PNHR/BB/empreendimentos com separador pipe'::text as fonte_minio_staging,
        p[3]::text as apf,
        p[3]::text as contrato,
        p[3]::text as codigo_empreendimento,
        p[4]::text as nome_empreendimento,
        p[9]::text as codigo_ibge_municipio,
        p[10]::text as municipio,
        p[11]::text as uf,
        'Entidade Organizadora'::text as responsavel_tipo,
        p[13]::text as responsavel_id,
        p[12]::text as responsavel_nome,
        'Banco do Brasil'::text as agente_financeiro,
        1::integer as quantidade_empreendimentos,
        1::integer as quantidade_contratos,
        {{ parse_int('p[15]') }} as quantidade_uh,
        {{ parse_int('p[16]') }} as quantidade_uh_entregues,
        {{ parse_financial_value('p[17]') }} as valor_contratado,
        {{ parse_financial_value('p[31]') }} as valor_desembolsado,
        {{ parse_numeric('p[33]', 'numeric(10, 2)') }} as percentual_execucao_fisica,
        {{ parse_numeric('p[34]', 'numeric(10, 2)') }} as percentual_execucao_financeira,
        p[38]::text as status_operacional,
        {{ target.schema }}.parse_date_br(p[1]) as dt_referencia,
        {{ target.schema }}.parse_date_br(p[8]) as dt_contratacao,
        null::date as dt_inicio_obra,
        null::date as dt_previsao_entrega,
        {{ target.schema }}.parse_date_br(p[40]) as dt_entrega,
        coalesce({{ target.schema }}.parse_date_br(p[40]), {{ target.schema }}.parse_date_br(p[39]), {{ target.schema }}.parse_date_br(p[1])) as dt_ultima_atualizacao,
        'Fonte rural BB carregada com registro em coluna unica; tratada via separador pipe.'::text as observacao_silver,
        current_timestamp as dt_silver
    from bb
    where array_length(p, 1) >= 40
),

caixa_padronizado as (
    select
        md5(concat_ws('|', 'rural-caixa', p[4], p[1])) as id_silver_frente,
        'Minha Casa Minha Vida'::text as programa,
        'Rural'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'PNHR Rural CAIXA'::text as linha_mcmv,
        'empreendimento_apf_pipe'::text as grao_registro,
        'sftp'::text as fonte_camada,
        'sftp'::text as fonte_schema,
        'int065_ministeriocidades_pnhr_caixa_empreendimentos_20240830'::text as fonte_tabela,
        'GEFUS/PNHR/CAIXA/empreendimentos com separador pipe'::text as fonte_minio_staging,
        p[4]::text as apf,
        p[4]::text as contrato,
        p[3]::text as codigo_empreendimento,
        p[5]::text as nome_empreendimento,
        p[9]::text as codigo_ibge_municipio,
        p[10]::text as municipio,
        p[11]::text as uf,
        'Entidade Organizadora'::text as responsavel_tipo,
        p[13]::text as responsavel_id,
        p[12]::text as responsavel_nome,
        'CAIXA'::text as agente_financeiro,
        1::integer as quantidade_empreendimentos,
        1::integer as quantidade_contratos,
        {{ parse_int('p[16]') }} as quantidade_uh,
        {{ parse_int('p[17]') }} as quantidade_uh_entregues,
        {{ parse_financial_value('p[18]') }} as valor_contratado,
        {{ parse_financial_value('p[31]') }} as valor_desembolsado,
        {{ parse_numeric('p[36]', 'numeric(10, 2)') }} as percentual_execucao_fisica,
        null::numeric(10, 2) as percentual_execucao_financeira,
        p[37]::text as status_operacional,
        {{ target.schema }}.parse_date_br(p[1]) as dt_referencia,
        {{ target.schema }}.parse_date_br(p[8]) as dt_contratacao,
        null::date as dt_inicio_obra,
        null::date as dt_previsao_entrega,
        {{ target.schema }}.parse_date_br(p[39]) as dt_entrega,
        coalesce({{ target.schema }}.parse_date_br(p[39]), {{ target.schema }}.parse_date_br(p[38]), {{ target.schema }}.parse_date_br(p[1])) as dt_ultima_atualizacao,
        'Fonte rural CAIXA carregada com registro em coluna unica; tratada via separador pipe.'::text as observacao_silver,
        current_timestamp as dt_silver
    from caixa
    where array_length(p, 1) >= 40
)

select * from bb_padronizado
union all
select * from caixa_padronizado
