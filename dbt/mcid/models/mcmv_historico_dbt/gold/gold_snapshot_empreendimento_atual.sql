{{ config(materialized="table") }}

-- Snapshot corrente (estado atual) derivado da silver historica por frente.
-- Mantem apenas o ultimo mes (dt_referencia) por (frente, apf). O id_historico_snapshot
-- herdado identifica unicamente a versao corrente de cada empreendimento.
-- Consolidado apenas (filtravel por frente_mcmv) — nao ha versao por frente.
--
-- Uniao direta das 3 silvers por frente (FAR/FDS/Rural) — antes lia do helper
-- silver_mcmv_historico_empreendimento, aposentado na convencao 2026-09-04
-- (cada frente materializa como silver_historico_empreendimento no schema da
-- propria frente; nao ha mais um schema unico onde um union all resolveria
-- sozinho).
with
    consolidado as (
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_far') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
    ),

    ultimo as (
        select
            *,
            row_number() over (
                partition by frente_mcmv, apf order by dt_referencia desc
            ) as rn
        from consolidado
    )

select
    id_historico_snapshot,
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    'empreendimento'::text as grao_registro,
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
    situacao_canonica,
    regiao_sigla,
    regiao_nome,
    dt_silver
from ultimo
where rn = 1
