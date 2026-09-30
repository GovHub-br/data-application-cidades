{{ config(materialized="table") }}

-- PRATA — desembolso mensal do FGTS por contrato Pró-Moradia, a partir de
-- bronze_sftp_pro_moradia_desembolsos (ledger cumulativo GEAVO, TODOS os 38
-- snapshots semanais).
--
-- `inner join` com prata_hist_pro_moradia_contrato por
-- `cod_contrato = codigo_contrato` restringe ao universo Pró-Moradia (D4 do
-- design.md) — evita reprocessar os ~1,6M linhas/semana que não são desta
-- frente.
--
-- ACHADO na implementação, não previsto no design.md original: a fonte NÃO
-- tem grão (cod_contrato, dte_ano, dte_mes_ref) único nem DENTRO de um único
-- snapshot — ~0,3% dos grupos Pró-Moradia (15 de 5.341 no snapshot mais
-- recente) têm 2-4 linhas na MESMA competência do MESMO snapshot (múltiplas
-- parcelas liberadas no mês). Um dedup por `row_number()`/`dt_referencia`
-- sem critério de desempate (como o D2 original descrevia) escolheria uma
-- dessas transações arbitrariamente, descartando as demais — ~R$1,5 mi
-- perdidos só no snapshot mais recente, de R$3,75 bi. Por isso o dedup é em
-- 2 passos: (1) SOMA `vlr_liberado` por (cod_contrato, dte_ano, dte_mes_ref,
-- dt_referencia) — preserva o total liberado de cada competência DENTRO de
-- cada snapshot; (2) só ENTÃO deduplica ENTRE snapshots (D2), mantendo o
-- `dt_referencia` mais recente por competência — a competência pode ter
-- sido corrigida num snapshot posterior.
{% set bronze = ref('bronze_sftp_pro_moradia_desembolsos') %}
{% set contratos = ref('prata_hist_pro_moradia_contrato') %}

with

    filtrado as (
        select
            b.cod_contrato,
            nullif(trim(b.dte_ano), '') as dte_ano,
            nullif(trim(b.dte_mes_ref), '') as dte_mes_ref,
            {{ parse_hist_double('b.vlr_liberado') }} as valor_liberado,
            b.dt_referencia,
            b.source_file
        from {{ bronze }} as b
        inner join {{ contratos }} as c on b.cod_contrato = c.codigo_contrato
        where nullif(trim(b.cod_contrato), '') is not null
    ),

    agregado_por_snapshot as (
        -- passo 1: soma transações da MESMA competência dentro do MESMO snapshot
        select
            cod_contrato,
            dte_ano,
            dte_mes_ref,
            dt_referencia,
            max(source_file) as source_file,
            sum(valor_liberado) as valor_liberado
        from filtrado
        group by cod_contrato, dte_ano, dte_mes_ref, dt_referencia
    ),

    dedup as (
        -- passo 2: entre snapshots, mantém a revisão mais recente da competência (D2)
        select
            *,
            row_number() over (
                partition by cod_contrato, dte_ano, dte_mes_ref order by dt_referencia desc
            ) as rn
        from agregado_por_snapshot
    )

select
    'Pró-Moradia'::text as frente_mcmv,
    cod_contrato::text as cod_contrato,
    dte_ano::text as dte_ano,
    dte_mes_ref::text as dte_mes_ref,
    try_cast(valor_liberado as numeric(15, 2)) as valor_liberado,
    dt_referencia,
    source_file,
    md5(
        concat_ws('|', cod_contrato, dte_ano, dte_mes_ref, cast(dt_referencia as varchar))
    ) as hash_linha,
    current_timestamp as dt_ingest
from dedup
where rn = 1
