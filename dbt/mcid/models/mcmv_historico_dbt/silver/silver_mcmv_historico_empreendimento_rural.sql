{{ config(materialized="table") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente Rural (PNHR).
--
-- SFTP  — bronze_mcmv_historico_empreendimento_sftp, interfaces INT057
-- (PNHR BB) e INT065 (PNHR CAIXA). Janela 2019-12 → atual.
-- SNH   — bronze_mcmv_historico_empreendimento_snh, modalidade = 'RURAL'
-- (cobre 'RURAL' da CAIXA e 'Rural' do BB). Janela 2024-06 → atual.
--
-- Grão: empreendimento × mês. Dedup por (frente_mcmv, apf, dt_referencia).
-- Precedência SNH na janela sobreposta (D6). Ver
-- models/docs/entregas/separacao-silver-historico-por-frente.md.
--
-- Obs.: INT057 tem a coluna temporal com nome inconsistente entre entregas
-- (idt_movimento vs dt_movimento) — tratado com coalesce.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
{% set sftp = ref('bronze_mcmv_historico_empreendimento_sftp') %}
{% set snh = ref('bronze_mcmv_historico_empreendimento_snh') %}

with

    rural_bb as (  -- INT057
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
            {{ parse_hist_bigint('qt_unidades') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_double('vr_investimento') }} as valor_contratado,
            {{ parse_hist_double('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('pc_execucao_fisica_obra') }}
            as percentual_execucao_fisica,
            nullif(trim(no_situacao_obra), '')::text as status_operacional,
            {{ parse_hist_date('dt_contrato') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            {{ parse_hist_date('dt_efetiva_conclusao') }} as dt_entrega,
            dt_referencia,
            {{ parse_hist_date('coalesce(idt_movimento, dt_movimento)') }}
            as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ sftp }}
        where
            fonte_interface = 'INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS'
            and nullif(trim(nu_contrato_empreendimento), '') is not null
    ),

    rural_caixa as (  -- INT065
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
            {{ parse_hist_bigint('qtde_unidades') }} as quantidade_uh,
            {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
            {{ parse_hist_double('vr_investimento_pnhr') }} as valor_contratado,
            {{ parse_hist_double('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('pc_obra_realizado') }} as percentual_execucao_fisica,
            nullif(trim(no_situacao_obra), '')::text as status_operacional,
            {{ parse_hist_date('dt_contrato') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            {{ parse_hist_date('dt_efetiva_conclusao') }} as dt_entrega,
            dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ sftp }}
        where
            fonte_interface = 'INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS'
            and nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: bb_*_pnhr_* mensal (2014-10 → 2018-07) — único sinal de Rural pré-2019.
    snh_rural as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'Rural'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'PNHR Rural'::text as linha_mcmv,
            'empreendimento_mes'::text as grao_registro,
            case
                when upper(nullif(trim(agente_financeiro::text), '')) like 'BB%'
                then 'Banco do Brasil'
                when upper(nullif(trim(agente_financeiro::text), '')) like 'CAIXA%'
                then 'CAIXA'
                when agente_arquivo = 'BB'
                then 'Banco do Brasil'
                when agente_arquivo = 'CAIXA'
                then 'CAIXA'
            end::text as agente_financeiro,
            nullif(trim(apf::text), '')::text as apf,
            nullif(trim(apf::text), '')::text as codigo_empreendimento,
            nullif(trim(nome_empreendimento::text), '')::text as nome_empreendimento,
            nullif(trim(codigo_ibge_do_municipio::text), '')::text
            as codigo_ibge_municipio,
            nullif(trim(municipio::text), '')::text as municipio,
            upper(nullif(trim(uf::text), ''))::text as uf,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            coalesce(
                {{ parse_hist_bigint('uh_contratadas') }},
                {{ parse_hist_bigint('uhs_contratadas') }}
            ) as quantidade_uh,
            coalesce(
                {{ parse_hist_bigint('uh_entregues') }},
                {{ parse_hist_bigint('uhs_entregues') }}
            ) as quantidade_uh_entregues,
            {{ parse_hist_double('valor_contratado') }} as valor_contratado,
            {{ parse_hist_double('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_hist_double('exec') }} as percentual_execucao_fisica,
            nullif(trim(situacao_do_empreendimento::text), '')::text
            as status_operacional,
            {{ parse_hist_date('data_de_contratacao') }} as dt_contratacao,
            null::date as dt_inicio_obra,
            {{ parse_hist_date('dt_entrega') }} as dt_entrega,
            dt_referencia,
            {{ parse_hist_date('data_de_movimento') }} as dt_movimento,
            'snh'::text as fonte_serie,
            ('SNH_dados_prioritarios_af_' || lower(coalesce(agente_arquivo, 'na')))::text
            as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ snh }}
        where
            upper(nullif(trim(modalidade::text), '')) = 'RURAL'
            and nullif(trim(apf::text), '') is not null
    ),

    unioned as (
        select *
        from rural_bb
        union all
        select *
        from rural_caixa
        union all
        select *
        from snh_rural
    ),

    enriquecido as (
        select
            *,
            max(responsavel_id) over grao as responsavel_id_grao,
            max(responsavel_nome) over grao as responsavel_nome_grao,
            max(dt_movimento) over grao as dt_movimento_grao
        from unioned
        window grao as (partition by frente_mcmv, apf, dt_referencia)
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by frente_mcmv, apf, dt_referencia
                order by case fonte_serie when 'snh' then 0 else 1 end, source_file
            ) as rn
        from enriquecido
    )

select
    md5(
        concat_ws(
            '|', 'empreendimento', frente_mcmv, coalesce(apf, ''), dt_referencia::text
        )
    ) as id_historico_snapshot,
    md5(concat_ws('|', programa, frente_mcmv, coalesce(apf, ''))) as id_negocio_historico,
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
    coalesce(responsavel_id, responsavel_id_grao) as responsavel_id,
    coalesce(responsavel_nome, responsavel_nome_grao) as responsavel_nome,
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
    coalesce(dt_movimento, dt_movimento_grao) as dt_movimento,
    fonte_serie,
    fonte_tabela,
    source_file,
    hash_linha,
    dt_ingest,
    current_timestamp as dt_silver
from dedup
where rn = 1
