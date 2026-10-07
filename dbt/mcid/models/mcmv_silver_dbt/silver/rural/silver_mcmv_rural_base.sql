{{ config(enabled=false) }}
{# ============================================================================
   DESATIVADO — domínio mcmv_silver_dbt (legado).
   Colisão de `alias="silver_historico_base"` entre as 10 frentes: com o bloco
   `mcmv_silver_dbt` do dbt_project.yml comentado, todas resolvem para o schema
   default e colidem no parse ("two resources with identical database
   representation"). Código original preservado abaixo, inerte.
   Reativar: remova o `config(enabled=false)` acima, descomente o bloco abaixo
   e restaure o bloco `mcmv_silver_dbt` no dbt_project.yml.
   ============================================================================ #}
{#
{{ config(materialized="table", alias="silver_historico_base") }}

-- Base silver da frente Rural (materializa como empreendimento_rural.silver_historico_base).
--
-- REPONTADA (change consolidar-schemas-historico-reloginho, D7): consome
-- `ouro.ouro_historico_snapshot_empreendimento_atual` filtrado a
-- `frente_mcmv = 'Rural'` — já é o retrato corrente por APF com LOCF resolvido
-- (fase mais avançada + dt_referencia mais recente, com forward-fill de
-- valor/responsável na cauda). Antes lia `empreendimento_rural.silver_rural_empreendimento`
-- (pilha "atual" aposentada — fork mais pobre da pilha dos colegas em prod, D6).
--
-- Ganhos do repontamento:
--   - `dt_referencia` deixa de ser `null::date` (o snapshot traz o mês real);
--   - `quantidade_uh_entregues` vem da coluna homônima do eixo (100% fill),
--     não de `qt_uh_alienadas` (alienada ≠ entregue);
--   - `linha_mcmv` herda a granularidade do eixo (PNHR Rural BB / CAIXA / Rural);
--   - `dt_entrega` = `dt_entrega_uh` (split BREAKING da change
--     enriquecer-datas-acompanhamento-historico);
--   - cobertura sobe de ~9.474 para ~10.707 APFs.
--
-- `dt_previsao_entrega` sai NULL (0% no eixo p/ Rural) — já era 0% no braço
-- anterior em prod (mcmv_silver.silver_mcmv_rural_base, 9.474 linhas). Perda zero.
-- `fase_empreendimento` segue NULL (0% no eixo p/ Rural).
select
    md5(concat_ws('|', 'rural', apf)) as id_silver_frente,
    'Minha Casa Minha Vida'::text as programa,
    'Rural'::text as frente_mcmv,
    'Subsidiada'::text as grupo_linha,
    linha_mcmv::text as linha_mcmv,
    'empreendimento_apf'::text as grao_registro,
    'ouro'::text as fonte_camada,
    'ouro'::text as fonte_schema,
    'ouro_historico_snapshot_empreendimento_atual'::text as fonte_tabela,
    'eixo histórico (INT057/INT065 + SNH) via ouro_historico_snapshot_empreendimento_atual'::text
    as fonte_minio_staging,
    apf::text as apf,
    apf::text as contrato,
    codigo_empreendimento::text as codigo_empreendimento,
    null::text as fase_empreendimento,
    nome_empreendimento::text as nome_empreendimento,
    codigo_ibge_municipio::text as codigo_ibge_municipio,
    municipio::text as municipio,
    uf::text as uf,
    'Entidade Organizadora'::text as responsavel_tipo,
    coalesce(nu_cnpj_entidade, responsavel_id)::text as responsavel_id,
    coalesce(no_entidade_organizadora, responsavel_nome)::text as responsavel_nome,
    agente_financeiro::text as agente_financeiro,
    1::integer as quantidade_empreendimentos,
    1::integer as quantidade_contratos,
    quantidade_uh::integer as quantidade_uh,
    quantidade_uh_entregues::integer as quantidade_uh_entregues,
    valor_contratado::numeric(15, 2) as valor_contratado,
    valor_desembolsado::numeric(15, 2) as valor_desembolsado,
    percentual_execucao_fisica::numeric(10, 2) as percentual_execucao_fisica,
    percentual_execucao_financeira::numeric(10, 2) as percentual_execucao_financeira,
    status_operacional::text as status_operacional,
    dt_referencia::date as dt_referencia,
    dt_contratacao::date as dt_contratacao,
    dt_inicio_obra::date as dt_inicio_obra,
    dt_previsao_entrega::date as dt_previsao_entrega,
    dt_entrega_uh::date as dt_entrega,
    coalesce(
        dt_entrega_uh,
        dt_conclusao_obra,
        dt_previsao_entrega,
        dt_ultima_liberacao,
        dt_contratacao
    )::date as dt_ultima_atualizacao,
    'Rural vem de ouro.ouro_historico_snapshot_empreendimento_atual (frente_mcmv = Rural).'::text
    as observacao_silver,
    current_timestamp as dt_silver
from {{ ref("ouro_historico_snapshot_empreendimento_atual") }}
where frente_mcmv = 'Rural' and apf is not null
#}
