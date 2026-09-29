{{ config(materialized="table") }}

-- PRATA — paralisação de contratos Pró-Moradia ao longo do tempo, a partir
-- de bronze_sftp_pro_moradia_paralisacoes (retrato semanal do estado ATIVO
-- de paralisação, sem coluna de competência).
--
-- Diferente de prata_pro_moradia_historico_desembolso_mensal/execucao_obra,
-- esta prata NÃO deduplica para o estado mais recente (D3 do design.md):
-- preserva uma linha por (cod_contrato, dt_referencia) — cada semana em que
-- o contrato apareceu na lista de paralisados vira uma observação distinta.
-- Um contrato que deixa de aparecer a partir de um snapshot simplesmente não
-- tem mais linha dali em diante — não há marcação explícita de "resolvido".
--
-- ACHADO na implementação (task 6.2, build completo dos 38 snapshots): o
-- snapshot de 2026-06-19 tem 18 contratos Pró-Moradia com 2 linhas
-- IDÊNTICAS em TODAS as colunas na fonte (inclusive `_source_hash`) — defeito
-- pontual de exportação daquela semana na origem, não 2 observações
-- distintas (confirmado célula a célula). Não visível nos 2 snapshots
-- extremos checados na task 1.2 (só neles não há garantia de cobrir TODOS os
-- 38). Um `row_number()` extra colapsa esse duplicata exata sem perda de
-- informação (o conteúdo é idêntico; a escolha de qual linha sobra é
-- arbitrária mas irrelevante).
-- `inner join` com prata_pro_moradia_historico_contrato por
-- `cod_contrato = codigo_contrato` restringe ao universo Pró-Moradia (D4).
{% set bronze = ref('bronze_sftp_pro_moradia_paralisacoes') %}
{% set contratos = ref('prata_pro_moradia_historico_contrato') %}

with

    filtrado as (
        select b.*
        from {{ bronze }} as b
        inner join {{ contratos }} as c on b.cod_contrato = c.codigo_contrato
        where nullif(trim(b.cod_contrato), '') is not null
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by cod_contrato, dt_referencia order by hash_linha
            ) as rn
        from filtrado
    )

select
    'Pró-Moradia'::text as frente_mcmv,
    nullif(trim(cod_contrato), '')::text as cod_contrato,
    nullif(trim(dias_sem_evolucao), '')::text as dias_sem_evolucao,
    nullif(trim(faixa_paralisacao), '')::text as faixa_paralisacao,
    nullif(trim(ultimo_motivo_paralisacao), '')::text as ultimo_motivo_paralisacao,
    nullif(trim(situacao_atual), '')::text as situacao_atual,
    nullif(trim(data_base), '')::text as data_base,
    dt_referencia,
    source_file,
    hash_linha,
    dt_ingest
from dedup
where rn = 1
