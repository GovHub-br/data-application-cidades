{{ config(materialized="table") }}

-- Serie historica mensal de empreendimentos MCMV (FAR, Entidades/FDS e Rural/PNHR)
-- a partir das tabelas de interface do SFTP (INT040, INT054, INT059, INT057, INT065)
-- lidas do MinIO staging/ via DuckDB. Grao: empreendimento x mes.
--
-- dt_referencia vem da data do NOME DO ARQUIVO (mais confiavel que dt_movimento,
-- ver docs/entregas/issue-130-pendencias-encoding-canonicalizacao-sftp-minio.md).
-- dt_movimento e mantido como campo auxiliar (cast + coalesce de vazios).
--
-- Arquivos de reentrega (sufixo _0000, _V2, ...) e de VALIDACAO sao excluidos
-- para evitar duplicidade de APF x mes. Alem disso, o grao (frente, APF, mes)
-- e deduplicado via row_number() porque algumas fontes trazem APFs repetidos
-- no mesmo snapshot. A canonicalizacao definitiva fica pendente (ver P2 do doc).
--
-- Valores numericos estao em formato brasileiro (13.898.046,25): remove-se o
-- separador de milhar '.' e troca-se a virgula por ponto antes do cast.
--
-- Obs.: INT057 tem a coluna temporal com nome inconsistente entre entregas
-- (idt_movimento vs dt_movimento) -> tratado com coalesce.

with

far_caixa as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'FAR'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FAR'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        'CAIXA'::text as agente_financeiro,
        nullif(trim(nu_apf), '')::text as apf,
        nullif(trim(nu_apf), '')::text as codigo_empreendimento,
        nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
        nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
        nullif(trim(no_municipio), '')::text as municipio,
        nullif(trim(sg_uf_muncicipio), '')::text as uf,
        nullif(trim(cnpj_proponente), '')::text as responsavel_id,
        nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
        try_cast(nullif(trim(qt_unidade_financiadas), '') as integer) as quantidade_uh,
        try_cast(nullif(trim(qt_unidades_entregues), '') as integer) as quantidade_uh_entregues,
        try_cast(replace(replace(nullif(trim(vr_investimento), ''), '.', ''), ',', '.') as double) as valor_contratado,
        try_cast(replace(replace(nullif(trim(vr_liberado), ''), '.', ''), ',', '.') as double) as valor_desembolsado,
        try_cast(replace(replace(nullif(trim(percentual_obra_realizado), ''), '.', ''), ',', '.') as double) as percentual_execucao_fisica,
        nullif(trim(situacao_obra_gefus), '')::text as status_operacional,
        try_cast(nullif(trim(dt_assinatura), '') as date) as dt_contratacao,
        try_cast(nullif(trim(dt_inicio_obra), '') as date) as dt_inicio_obra,
        try_cast(nullif(trim(dt_ultima_entrega), '') as date) as dt_entrega,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date as dt_referencia,
        try_cast(nullif(trim(dt_movimento), '') as date) as dt_movimento,
        'INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS'::text as fonte_tabela,
        filename as source_file
    from {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/INT040_*.parquet') }}
    where nullif(trim(nu_apf), '') is not null
      and regexp_matches(filename, '_\d{8}\.parquet$')
      and filename not ilike '%validacao%'
),

far_bb as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'FAR'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FAR'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        'Banco do Brasil'::text as agente_financeiro,
        nullif(trim(nu_apf), '')::text as apf,
        nullif(trim(nu_apf), '')::text as codigo_empreendimento,
        nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
        nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
        nullif(trim(no_municipio), '')::text as municipio,
        nullif(trim(sg_uf), '')::text as uf,
        nullif(trim(cnpj_proponente), '')::text as responsavel_id,
        nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
        try_cast(nullif(trim(qt_unidades_habitacionais), '') as integer) as quantidade_uh,
        try_cast(nullif(trim(qt_unidades_entregues), '') as integer) as quantidade_uh_entregues,
        try_cast(replace(replace(nullif(trim(vr_investimento), ''), '.', ''), ',', '.') as double) as valor_contratado,
        try_cast(replace(replace(nullif(trim(total_liberado_far), ''), '.', ''), ',', '.') as double) as valor_desembolsado,
        try_cast(replace(replace(nullif(trim(percentual_obra_realizado), ''), '.', ''), ',', '.') as double) as percentual_execucao_fisica,
        nullif(trim(situacao_obra), '')::text as status_operacional,
        try_cast(nullif(trim(dt_contratacao), '') as date) as dt_contratacao,
        try_cast(nullif(trim(dt_inicio_obra), '') as date) as dt_inicio_obra,
        try_cast(nullif(trim(dt_ultima_entrega), '') as date) as dt_entrega,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date as dt_referencia,
        try_cast(nullif(trim(dt_movimento), '') as date) as dt_movimento,
        'INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS'::text as fonte_tabela,
        filename as source_file
    from {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/INT054_*.parquet') }}
    where nullif(trim(nu_apf), '') is not null
      and regexp_matches(filename, '_\d{8}\.parquet$')
      and filename not ilike '%validacao%'
),

fds as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'Entidades'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FDS / Entidades'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        'CAIXA'::text as agente_financeiro,
        nullif(trim(nu_apf), '')::text as apf,
        nullif(trim(nu_apf), '')::text as codigo_empreendimento,
        nullif(trim(no_empreeendmento), '')::text as nome_empreendimento,
        nullif(trim(cod_municipio_ibge), '')::text as codigo_ibge_municipio,
        nullif(trim(no_municipio), '')::text as municipio,
        null::text as uf,
        nullif(trim(cnpj_proponente), '')::text as responsavel_id,
        nullif(trim(razao_social_proponente), '')::text as responsavel_nome,
        try_cast(nullif(trim(qt_unidade_financiadas), '') as integer) as quantidade_uh,
        null::integer as quantidade_uh_entregues,
        try_cast(replace(replace(nullif(trim(vr_investimento), ''), '.', ''), ',', '.') as double) as valor_contratado,
        try_cast(replace(replace(nullif(trim(vr_liberado), ''), '.', ''), ',', '.') as double) as valor_desembolsado,
        try_cast(replace(replace(nullif(trim(percentual_obra_realizado), ''), '.', ''), ',', '.') as double) as percentual_execucao_fisica,
        coalesce(nullif(trim(situacao_gefus), ''), nullif(trim(fase_contrato), ''))::text as status_operacional,
        try_cast(nullif(trim(dt_assinatura), '') as date) as dt_contratacao,
        try_cast(nullif(trim(dt_inicio_obra), '') as date) as dt_inicio_obra,
        null::date as dt_entrega,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date as dt_referencia,
        try_cast(nullif(trim(dt_movimento), '') as date) as dt_movimento,
        'INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS'::text as fonte_tabela,
        filename as source_file
    from {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/INT059_*.parquet') }}
    where nullif(trim(nu_apf), '') is not null
      and regexp_matches(filename, '_\d{8}\.parquet$')
      and filename not ilike '%validacao%'
),

rural_bb as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'Rural'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'PNHR Rural BB'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        'Banco do Brasil'::text as agente_financeiro,
        nullif(trim(nu_contrato_empreendimento), '')::text as apf,
        nullif(trim(nu_contrato_empreendimento), '')::text as codigo_empreendimento,
        nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
        nullif(trim(co_municipio_ibge), '')::text as codigo_ibge_municipio,
        nullif(trim(no_municipio), '')::text as municipio,
        nullif(trim(sg_uf), '')::text as uf,
        nullif(trim(nu_cnpj_entidade), '')::text as responsavel_id,
        nullif(trim(no_entidade_organizadora), '')::text as responsavel_nome,
        try_cast(nullif(trim(qt_unidades), '') as integer) as quantidade_uh,
        try_cast(nullif(trim(qt_unidades_entregues), '') as integer) as quantidade_uh_entregues,
        try_cast(replace(replace(nullif(trim(vr_investimento), ''), '.', ''), ',', '.') as double) as valor_contratado,
        try_cast(replace(replace(nullif(trim(vr_liberado), ''), '.', ''), ',', '.') as double) as valor_desembolsado,
        try_cast(replace(replace(nullif(trim(pc_execucao_fisica_obra), ''), '.', ''), ',', '.') as double) as percentual_execucao_fisica,
        nullif(trim(no_situacao_obra), '')::text as status_operacional,
        try_cast(nullif(trim(dt_contrato), '') as date) as dt_contratacao,
        null::date as dt_inicio_obra,
        try_cast(nullif(trim(dt_efetiva_conclusao), '') as date) as dt_entrega,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date as dt_referencia,
        try_cast(nullif(trim(coalesce(idt_movimento, dt_movimento)), '') as date) as dt_movimento,
        'INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS'::text as fonte_tabela,
        filename as source_file
    from {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/INT057_*.parquet') }}
    where nullif(trim(nu_contrato_empreendimento), '') is not null
      and regexp_matches(filename, '_\d{8}\.parquet$')
      and filename not ilike '%validacao%'
),

rural_caixa as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'Rural'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'PNHR Rural CAIXA'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        'CAIXA'::text as agente_financeiro,
        nullif(trim(nu_apf), '')::text as apf,
        nullif(trim(nu_apf), '')::text as codigo_empreendimento,
        nullif(trim(no_empreendimento), '')::text as nome_empreendimento,
        nullif(trim(co_municipio_ibge), '')::text as codigo_ibge_municipio,
        nullif(trim(no_municipio), '')::text as municipio,
        nullif(trim(sg_uf), '')::text as uf,
        nullif(trim(nu_cnpj_entidade), '')::text as responsavel_id,
        nullif(trim(no_entidade_organizadora), '')::text as responsavel_nome,
        try_cast(nullif(trim(qtde_unidades), '') as integer) as quantidade_uh,
        try_cast(nullif(trim(qt_unidades_entregues), '') as integer) as quantidade_uh_entregues,
        try_cast(replace(replace(nullif(trim(vr_investimento_pnhr), ''), '.', ''), ',', '.') as double) as valor_contratado,
        try_cast(replace(replace(nullif(trim(vr_liberado), ''), '.', ''), ',', '.') as double) as valor_desembolsado,
        try_cast(replace(replace(nullif(trim(pc_obra_realizado), ''), '.', ''), ',', '.') as double) as percentual_execucao_fisica,
        nullif(trim(no_situacao_obra), '')::text as status_operacional,
        try_cast(nullif(trim(dt_contrato), '') as date) as dt_contratacao,
        null::date as dt_inicio_obra,
        try_cast(nullif(trim(dt_efetiva_conclusao), '') as date) as dt_entrega,
        strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date as dt_referencia,
        try_cast(nullif(trim(dt_movimento), '') as date) as dt_movimento,
        'INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS'::text as fonte_tabela,
        filename as source_file
    from {{ read_minio_staging_parquet_series('sftp/fabrica/GEFUS/**/INT065_*.parquet') }}
    where nullif(trim(nu_apf), '') is not null
      and regexp_matches(filename, '_\d{8}\.parquet$')
      and filename not ilike '%validacao%'
),

uniao as (
    select * from far_caixa
    union all
    select * from far_bb
    union all
    select * from fds
    union all
    select * from rural_bb
    union all
    select * from rural_caixa
),

dedup as (
    select *,
           row_number() over (partition by frente_mcmv, apf, dt_referencia order by source_file) as rn
    from uniao
)

select
    md5(concat_ws('|', 'empreendimento', frente_mcmv, coalesce(apf, ''), dt_referencia::text)) as id_historico_snapshot,
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    grao_registro,
    agente_financeiro,
    apf,
    codigo_empreendimento,
    nome_empreendimento,
    codigo_ibge_municipio,
    municipio,
    uf,
    responsavel_id,
    responsavel_nome,
    quantidade_uh,
    quantidade_uh_entregues,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    dt_inicio_obra,
    dt_entrega,
    dt_referencia,
    dt_movimento,
    fonte_tabela,
    source_file,
    current_timestamp as dt_silver
from dedup
where rn = 1
