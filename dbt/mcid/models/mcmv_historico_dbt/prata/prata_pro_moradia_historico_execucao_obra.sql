{{ config(materialized="table") }}

-- PRATA — execução física de obra por contrato Pró-Moradia, a partir de
-- bronze_sftp_pro_moradia_execucoes_obra (ledger cumulativo GEAVO, TODOS os
-- 38 snapshots semanais).
--
-- Mesmo padrão de prata_pro_moradia_historico_desembolso_mensal: `inner
-- join` com prata_pro_moradia_historico_contrato por
-- `cod_contrato = codigo_contrato` (D4), dedup por (cod_contrato,
-- dte_ano_mes_avaliacao) mantendo o `dt_referencia` mais recente (D2).
-- Confirmado (diferente de tab_desembolsos_fgts) que o grão da fonte é
-- único por (cod_contrato, dte_ano_mes_avaliacao) mesmo DENTRO de um único
-- snapshot — nenhuma duplicata no universo Pró-Moradia nos 2 snapshots
-- extremos checados (mais antigo e mais recente), então o `row_number`
-- simples é suficiente aqui.
-- `prc_prev_acum_mes`/`prc_real_acum_mes` expostos como vêm da fonte, sem
-- normalizar (open question do design.md, decisão adiada).
{% set bronze = ref('bronze_sftp_pro_moradia_execucoes_obra') %}
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
                partition by cod_contrato, dte_ano_mes_avaliacao order by dt_referencia desc
            ) as rn
        from filtrado
    )

select
    'Pró-Moradia'::text as frente_mcmv,
    nullif(trim(cod_contrato), '')::text as cod_contrato,
    nullif(trim(dte_ano_mes_avaliacao), '')::text as dte_ano_mes_avaliacao,
    {{ parse_hist_double('prc_prev_acum_mes') }} as percentual_previsto_acumulado_mes,
    {{ parse_hist_double('prc_real_acum_mes') }} as percentual_realizado_acumulado_mes,
    nullif(trim(cod_situacao_obra), '')::text as codigo_situacao_obra,
    nullif(trim(cod_execucao_obra), '')::text as codigo_execucao_obra,
    dt_referencia,
    source_file,
    hash_linha,
    dt_ingest
from dedup
where rn = 1
