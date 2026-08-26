{{ config(materialized="table") }}

-- Snapshot corrente (estado atual) derivado do modelo historico (decisao D2).
-- Mantem apenas o ultimo mes (dt_referencia) por (frente, apf). O id_historico_snapshot
-- herdado identifica unicamente a versao corrente de cada empreendimento.

with ultimo as (
    select *,
           row_number() over (partition by frente_mcmv, apf order by dt_referencia desc) as rn
    from {{ ref('historico_mcmv_empreendimentos_snapshot') }}
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
