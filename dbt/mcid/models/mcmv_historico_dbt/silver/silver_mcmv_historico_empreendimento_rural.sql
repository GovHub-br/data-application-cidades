{{ config(materialized="table", alias="silver_historico_empreendimento", schema="empreendimento_rural") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente Rural (PNHR).
--
-- SFTP  — bronzes por interface INT057 (PNHR BB) e INT065 (PNHR CAIXA).
-- Janela 2019-12 → atual.
-- SNH   — bronzes por agente (BB, CAIXA), modalidade = 'RURAL' (cobre
-- 'RURAL' da CAIXA e 'Rural' do BB). Janela 2024-06 → atual.
--
-- Desde a change pipeline-bronze-historica-destino-trocavel (D5) cada fonte é
-- uma TABELA POR FAMÍLIA; a união com projeção explícita acontece aqui.
--
-- Grão: empreendimento × mês. Dedup por (frente_mcmv, apf, dt_referencia).
-- Precedência SNH na janela sobreposta (D6). Ver
-- models/docs/entregas/separacao-silver-historico-por-frente.md.
--
-- Obs.: INT057 tem a coluna temporal com nome inconsistente entre entregas
-- (idt_movimento vs dt_movimento) — tratado com coalesce.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. Ver models/mcmv_historico_dbt/README.md para a
-- ordem de build exigida por coalesce_present.
{% set int057 = ref('bronze_mcmv_historico_empreendimento_int057') %}
{% set int065 = ref('bronze_mcmv_historico_empreendimento_int065') %}
{% set snh_familias = familias_snh_empreendimento() %}

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
        from {{ int057 }}
        where nullif(trim(nu_contrato_empreendimento), '') is not null
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
        from {{ int065 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: bb_*_pnhr_* mensal (2014-10 → 2018-07) — único sinal de Rural pré-2019.
{% for f in snh_familias %}
    snh_rural_{{ f.nome | lower }} as (
{{ silver_historico_snh_arm(ref(f.modelo), 'Rural', 'PNHR Rural', 'RURAL') }}
    ),
{% endfor %}
    unioned as (
        select *
        from rural_bb
        union all
        select *
        from rural_caixa
        {% for f in snh_familias %}
        union all
        select *
        from snh_rural_{{ f.nome | lower }}
        {% endfor %}
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
