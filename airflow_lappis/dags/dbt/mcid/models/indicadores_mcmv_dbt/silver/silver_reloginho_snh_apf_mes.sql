{{ config(materialized="table") }}

-- SILVER do reloginho MCMV (grupo A) — série mensal SNH tratada e deduplicada.
--
-- Lê a bronze (bronze_reloginho_snh_serie_mensal) e aplica:
--   * tipagem  — texto -> bigint (UH) / date (datas);
--   * normalização de domínio — agente_financeiro em maiúsculas (BB/CAIXA) e
--     frente_mcmv canônica a partir de `modalidade` (FAR / Entidades / Rural —
--     resolve o RURAL vs Rural entre CAIXA e BB, decisão #6 do
--     issue-130-resumo-final);
--   * deduplicação — as tabelas `historico_recente_*` trazem cada APF 2x no
--     mesmo snapshot (#130 Fase 3). row_number() por (agente, apf,
--     dt_referencia) mantendo rn = 1 — MESMA chave validada na reconciliação
--     contra a referência #66 (diff 0,000%). Reentregas do mesmo mês
--     (sufixos vsNN / correcao) são desempatadas por prioridade_reentrega.
--
-- Grão de saída: uma linha por (agente_financeiro, apf, dt_referencia), com a
-- frente_mcmv da linha sobrevivente. Alimenta as golds indicadores_reloginho
-- (total por agente) e indicadores_reloginho_frente (quebra por frente).
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).

with

bronze as (
    select * from {{ ref("bronze_reloginho_snh_serie_mensal") }}
),

tipado as (
    select
        -- Usa a coluna da fonte (não o nome do arquivo) para não recuperar
        -- linhas que a versão anterior do gold descartava — preserva a
        -- reconciliação exata contra a referência #66. agente_arquivo fica
        -- disponível na bronze para auditoria.
        upper(nullif(trim(agente_financeiro::text), '')) as agente_financeiro,
        case upper(nullif(trim(modalidade::text), ''))
            when 'FAR' then 'FAR'
            when 'ENTIDADES' then 'Entidades'
            when 'FDS' then 'Entidades'
            when 'FDS / ENTIDADES' then 'Entidades'
            when 'RURAL' then 'Rural'
            when 'PNHR' then 'Rural'
            else nullif(trim(modalidade::text), '')
        end as frente_mcmv,
        nullif(trim(apf::text), '') as apf,
        dt_referencia,
        try_cast(nullif(trim(data_de_movimento::text), '') as date) as data_de_movimento,
        try_cast(nullif(trim(uh_contratadas::text), '') as bigint) as uh_contratadas,
        try_cast(nullif(trim(uh_entregues::text), '') as bigint) as uh_entregues,
        try_cast(nullif(trim(uh_vigentes::text), '') as bigint) as uh_vigentes,
        upper(nullif(trim(uf::text), '')) as uf,
        nullif(trim(codigo_ibge_do_municipio::text), '') as codigo_ibge_municipio,
        nullif(trim(municipio::text), '') as municipio,
        nullif(trim(situacao_do_empreendimento::text), '') as status_operacional,
        try_cast(nullif(trim(data_de_contratacao::text), '') as date) as dt_contratacao,
        prioridade_reentrega,
        source_file,
        hash_linha
    from bronze
    where nullif(trim(apf::text), '') is not null
),

dedup as (
    select
        *,
        row_number() over (
            partition by agente_financeiro, apf, dt_referencia
            order by prioridade_reentrega desc, data_de_movimento nulls last, source_file
        ) as rn
    from tipado
    where dt_referencia is not null
      and agente_financeiro is not null
)

select
    agente_financeiro,
    frente_mcmv,
    apf,
    dt_referencia,
    uh_contratadas,
    uh_entregues,
    uh_vigentes,
    uf,
    codigo_ibge_municipio,
    municipio,
    status_operacional,
    dt_contratacao,
    data_de_movimento,
    source_file,
    hash_linha
from dedup
where rn = 1
