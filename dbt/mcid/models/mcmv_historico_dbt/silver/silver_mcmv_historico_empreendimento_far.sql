{{ config(materialized="table") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente FAR.
--
-- Une as duas fontes de história por empreendimento, no contrato semântico
-- comum (ver models/docs/entregas/separacao-silver-historico-por-frente.md):
--
--   SFTP  — bronze_mcmv_historico_empreendimento_sftp, interfaces INT040
--           (FAR CAIXA) e INT054 (FAR BB). Janela 2019-12 → atual.
--   SNH   — bronze_mcmv_historico_empreendimento_snh, linhas com
--           modalidade = 'FAR'. Janela 2024-06 → atual.
--
-- Grão de saída: empreendimento × mês de referência (dt_referencia). Deduplica
-- por (frente_mcmv, apf, dt_referencia). Na janela sobreposta (2024-06 →
-- 2024-11) prevalece a linha do SNH (D6): mais rica em situação/fase de obra.
-- Colunas que só o SFTP tem (dt_inicio_obra, responsável) são preservadas via
-- coalesce dentro do grão ANTES da escolha da linha.
--
-- Numéricos em formato brasileiro (13.898.046,25) e dot-decimal são absorvidos
-- por parse_hist_double / parse_hist_bigint.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).

{% set sftp = ref('bronze_mcmv_historico_empreendimento_sftp') %}
{% set snh = ref('bronze_mcmv_historico_empreendimento_snh') %}

with

far_caixa as (  -- INT040
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
        {{ parse_hist_bigint('qt_unidade_financiadas') }} as quantidade_uh,
        {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
        {{ parse_hist_double('vr_investimento') }} as valor_contratado,
        {{ parse_hist_double('vr_liberado') }} as valor_desembolsado,
        {{ parse_hist_double('percentual_obra_realizado') }} as percentual_execucao_fisica,
        nullif(trim(situacao_obra_gefus), '')::text as status_operacional,
        {{ parse_hist_date('dt_assinatura') }} as dt_contratacao,
        {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
        {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega,
        dt_referencia,
        {{ parse_hist_date('dt_movimento') }} as dt_movimento,
        'sftp'::text as fonte_serie,
        fonte_interface::text as fonte_tabela,
        source_file,
        hash_linha,
        dt_ingest
    from {{ sftp }}
    where fonte_interface = 'INT040_MinisterioCidades_FAR_CAIXA_EMPREENDIMENTOS'
      and nullif(trim(nu_apf), '') is not null
),

far_bb as (  -- INT054
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
        {{ parse_hist_bigint('qt_unidades_habitacionais') }} as quantidade_uh,
        {{ parse_hist_bigint('qt_unidades_entregues') }} as quantidade_uh_entregues,
        {{ parse_hist_double('vr_investimento') }} as valor_contratado,
        {{ parse_hist_double('total_liberado_far') }} as valor_desembolsado,
        {{ parse_hist_double('percentual_obra_realizado') }} as percentual_execucao_fisica,
        nullif(trim(situacao_obra), '')::text as status_operacional,
        {{ parse_hist_date('dt_contratacao') }} as dt_contratacao,
        {{ parse_hist_date('dt_inicio_obra') }} as dt_inicio_obra,
        {{ parse_hist_date('dt_ultima_entrega') }} as dt_entrega,
        dt_referencia,
        {{ parse_hist_date('dt_movimento') }} as dt_movimento,
        'sftp'::text as fonte_serie,
        fonte_interface::text as fonte_tabela,
        source_file,
        hash_linha,
        dt_ingest
    from {{ sftp }}
    where fonte_interface = 'INT054_MinisterioCidades_FAR_BB_EMPREENDIMENTOS'
      and nullif(trim(nu_apf), '') is not null
),

-- fase 2: bb_YYYY_*_pj / _pj_pf (2015-2019) — preenche o gap entre entrada_bb
--         (2014-09) e o início da série SFTP (2019-12).
-- fase 2: int040/int054_ministeriocidades_* dentro do dump (2018) — estende a
--         série SFTP ~1 ano para trás (só adicionar glob à bronze SFTP).

snh_far as (
    select
        'Minha Casa Minha Vida'::text as programa,
        'FAR'::text as frente_mcmv,
        'Subsidiada'::text as grupo_linha,
        'FAR'::text as linha_mcmv,
        'empreendimento_mes'::text as grao_registro,
        case
            when upper(nullif(trim(agente_financeiro::text), '')) like 'BB%' then 'Banco do Brasil'
            when upper(nullif(trim(agente_financeiro::text), '')) like 'CAIXA%' then 'CAIXA'
            when agente_arquivo = 'BB' then 'Banco do Brasil'
            when agente_arquivo = 'CAIXA' then 'CAIXA'
        end::text as agente_financeiro,
        nullif(trim(apf::text), '')::text as apf,
        nullif(trim(apf::text), '')::text as codigo_empreendimento,
        nullif(trim(nome_empreendimento::text), '')::text as nome_empreendimento,
        nullif(trim(codigo_ibge_do_municipio::text), '')::text as codigo_ibge_municipio,
        nullif(trim(municipio::text), '')::text as municipio,
        upper(nullif(trim(uf::text), ''))::text as uf,
        null::text as responsavel_id,
        null::text as responsavel_nome,
        coalesce({{ parse_hist_bigint('uh_contratadas') }}, {{ parse_hist_bigint('uhs_contratadas') }}) as quantidade_uh,
        coalesce({{ parse_hist_bigint('uh_entregues') }}, {{ parse_hist_bigint('uhs_entregues') }}) as quantidade_uh_entregues,
        {{ parse_hist_double('valor_contratado') }} as valor_contratado,
        {{ parse_hist_double('valor_desembolsado') }} as valor_desembolsado,
        {{ parse_hist_double('exec') }} as percentual_execucao_fisica,
        nullif(trim(situacao_do_empreendimento::text), '')::text as status_operacional,
        {{ parse_hist_date('data_de_contratacao') }} as dt_contratacao,
        null::date as dt_inicio_obra,
        {{ parse_hist_date('dt_entrega') }} as dt_entrega,
        dt_referencia,
        {{ parse_hist_date('data_de_movimento') }} as dt_movimento,
        'snh'::text as fonte_serie,
        ('SNH_dados_prioritarios_af_' || lower(coalesce(agente_arquivo, 'na')))::text as fonte_tabela,
        source_file,
        hash_linha,
        dt_ingest
    from {{ snh }}
    where upper(nullif(trim(modalidade::text), '')) = 'FAR'
      and nullif(trim(apf::text), '') is not null
),

unioned as (
    select * from far_caixa
    union all
    select * from far_bb
    union all
    select * from snh_far
),

-- D6: preserva colunas complementares (presentes só no SFTP) ao longo do grão
-- antes de escolher a linha vencedora.
enriquecido as (
    select
        *,
        max(dt_inicio_obra) over grao as dt_inicio_obra_grao,
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
    md5(concat_ws('|', 'empreendimento', frente_mcmv, coalesce(apf, ''), dt_referencia::text)) as id_historico_snapshot,
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
