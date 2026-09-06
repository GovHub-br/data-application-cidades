{{ config(materialized="table", alias="silver_historico_empreendimento", schema="empreendimentos_fds") }}

-- SILVER — série histórica mensal de empreendimentos MCMV da frente
-- FDS / Entidades.
--
-- SFTP  — bronze da interface INT059 (FDS CAIXA). Janela 2019-12 → atual.
-- SNH   — bronzes por agente (BB, CAIXA), modalidade = 'ENTIDADES'.
-- Janela 2024-06 → atual.
--
-- Desde a change pipeline-bronze-historica-destino-trocavel (D5) cada fonte é
-- uma TABELA POR FAMÍLIA; a união com projeção explícita acontece aqui.
--
-- Grão: empreendimento × mês. Dedup por (frente_mcmv, apf, dt_referencia).
-- Precedência SNH na janela sobreposta (D6). Ver
-- models/docs/entregas/separacao-silver-historico-por-frente.md.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`. Ver models/mcmv_historico_dbt/README.md para a
-- ordem de build exigida por coalesce_present.
{% set int059 = ref('bronze_mcmv_historico_empreendimento_int059') %}
{% set snh_familias = familias_snh_empreendimento() %}

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
        from {{ int059 }}
        where nullif(trim(nu_apf), '') is not null
    ),

    -- fase 2: min_cidades (grão empreendimento/contrato) traz FDS pré-2019 —
    -- acrescentar CTE lendo a bronze da série executiva filtrada.
{% for f in snh_familias %}
    snh_entidades_{{ f.nome | lower }} as (
{{ silver_historico_snh_arm(ref(f.modelo), 'Entidades', 'FDS / Entidades', 'ENTIDADES') }}
    ),
{% endfor %}
    unioned as (
        select *
        from fds_caixa
        {% for f in snh_familias %}
        union all
        select *
        from snh_entidades_{{ f.nome | lower }}
        {% endfor %}
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
    ),

    -- situacao_canonica + regiao_* por left join aos seeds de referência
    -- (D1/D3 da change serie-historica-situacao-obra-regiao): resolvidas uma
    -- vez, depois da união e da dedup — status_operacional e uf já estão no
    -- contrato comum de todos os braços (SFTP e SNH). status cru preservado.
    -- Obs.: o braço SFTP INT059 não traz uf (regiao_* fica nula nele; o braço
    -- SNH tem uf).
    enriquecido_dominio as (
        select
            d.*,
            case
                when nullif(trim(d.status_operacional), '') is null
                then null
                when lower(trim(d.status_operacional)) in ('null', 'nan')
                then null
                when ds.situacao_canonica is not null
                then ds.situacao_canonica
                else 'nao_mapeada'
            end as situacao_canonica,
            dr.regiao_sigla,
            dr.regiao_nome
        from dedup d
        left join {{ ref('dominio_status') }} ds
            on lower(trim(d.status_operacional)) = lower(trim(ds.valor_bruto))
        left join {{ ref('dominio_regiao_uf') }} dr
            on upper(trim(d.uf)) = upper(trim(dr.uf))
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
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    current_timestamp as dt_silver
from enriquecido_dominio
where rn = 1
