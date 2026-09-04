{{ config(materialized="table") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente
-- FDS / Entidades.
--
-- SFTP  — bronze_mcmv_historico_empreendimento_sftp, interface INT059
-- (FDS CAIXA). Janela 2019-12 → atual.
-- SNH   — bronze_mcmv_historico_empreendimento_snh, modalidade = 'ENTIDADES'.
-- Janela 2024-06 → atual.
--
-- Grão: empreendimento × mês. Dedup por (frente_mcmv, apf, dt_referencia).
-- Precedência SNH na janela sobreposta (D6). Ver
-- models/docs/entregas/separacao-silver-historico-por-frente.md.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
{% set sftp = ref('bronze_mcmv_historico_empreendimento_sftp') %}
{% set snh = ref('bronze_mcmv_historico_empreendimento_snh') %}

with

    fds_caixa as (  -- INT059
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
            {{ parse_hist_bigint('qt_unidade_financiadas') }} as quantidade_uh,
            null::bigint as quantidade_uh_entregues,
            {{ parse_hist_double('vr_investimento') }} as valor_contratado,
            {{ parse_hist_double('vr_liberado') }} as valor_desembolsado,
            {{ parse_hist_double('percentual_obra_realizado') }}
            as percentual_execucao_fisica,
            coalesce(
                nullif(trim(situacao_gefus), ''), nullif(trim(fase_contrato), '')
            )::text as status_operacional,
            {{ parse_hist_date('dt_assinatura') }} as dt_contratacao,
            {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
            null::date as dt_entrega,
            dt_referencia,
            {{ parse_hist_date('dt_movimento') }} as dt_movimento,
            'sftp'::text as fonte_serie,
            fonte_interface::text as fonte_tabela,
            source_file,
            hash_linha,
            dt_ingest
        from {{ sftp }}
        where
            fonte_interface = 'INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS'
            and nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: min_cidades (grão empreendimento/contrato) traz FDS pré-2019 —
    -- acrescentar CTE lendo a bronze da série executiva filtrada.
    snh_entidades as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'Entidades'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FDS / Entidades'::text as linha_mcmv,
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
            upper(nullif(trim(modalidade::text), '')) = 'ENTIDADES'
            and nullif(trim(apf::text), '') is not null
    ),

    unioned as (
        select *
        from fds_caixa
        union all
        select *
        from snh_entidades
    ),

    enriquecido as (
        select
            *,
            max(dt_inicio_obra) over grao as dt_inicio_obra_grao,
            max(responsavel_id) over grao as responsavel_id_grao,
            max(responsavel_nome) over grao as responsavel_nome_grao,
            max(dt_movimento) over grao as dt_movimento_grao,
            max(quantidade_uh_entregues) over grao as quantidade_uh_entregues_grao
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
    coalesce(
        quantidade_uh_entregues, quantidade_uh_entregues_grao
    ) as quantidade_uh_entregues,
    valor_contratado,
    valor_desembolsado,
    percentual_execucao_fisica,
    status_operacional,
    dt_contratacao,
    coalesce(dt_inicio_obra, dt_inicio_obra_grao) as dt_inicio_obra,
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
