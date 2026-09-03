{{ config(materialized="table") }}

-- Snapshot corrente (estado atual) derivado da silver historica consolidada.
-- Mantem apenas o ultimo mes (dt_referencia) por (frente, apf). O id_historico_snapshot
-- herdado identifica unicamente a versao corrente de cada empreendimento.
-- Consolidado apenas (filtravel por frente_mcmv) — nao ha versao por frente.

with ultimo as (
    select *,
           row_number() over (partition by frente_mcmv, apf order by dt_referencia desc) as rn
    from {{ ref('silver_mcmv_historico_empreendimento') }}
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
    dt_silver
from ultimo
where rn = 1
