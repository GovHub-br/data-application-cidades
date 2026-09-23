{{ config(materialized="table") }}

-- Prata: Desembolsos mensais do Pró-Moradia
-- Fonte: bronze_shpt_fgts_canal_desembolsos, restrita aos contratos de prata_pro_moradia_contrato.
--
-- Grão: contrato × competência. A origem tem 17 pares repetidos em 5.252 lançamentos (conferido
-- em 2026-09); somá-los é o correto, porque são liberações distintas no mesmo mês, e é o que
-- mantém o grão declarado. Valor negativo é estorno e entra com sinal: some do acumulado.
--
-- A série começa em 2000. Contrato assinado antes disso tem liberação anterior ao arquivo,
-- então o acumulado desta tabela é um PISO do desembolsado, não o total.

with
    contrato as (
        select cod_contrato, cod_contrato_dv from {{ ref("prata_pro_moradia_contrato") }}
    ),

    lancamento as (
        select
            trim(d.cod_contrato::text) as cod_contrato,
            trim(d.cod_contrato_dv::text) as cod_contrato_dv,
            make_date(
                {{ parse_int("d.dte_ano::text") }}, {{ parse_int("d.dte_mes_ref::text") }}, 1
            ) as dt_competencia,
            {{ parse_numeric("d.vlr_liberado::text", "numeric(15, 2)") }} as vr_liberado
        from {{ ref("bronze_shpt_fgts_canal_desembolsos") }} d
        join contrato c
            on c.cod_contrato = trim(d.cod_contrato::text)
            and c.cod_contrato_dv = trim(d.cod_contrato_dv::text)
    )

select
    cod_contrato,
    cod_contrato_dv,
    cod_contrato || '-' || cod_contrato_dv as contrato,
    dt_competencia,
    sum(vr_liberado) as vr_liberado,
    coalesce(sum(vr_liberado) filter (where vr_liberado < 0), 0) as vr_estornado,
    count(*) as qt_lancamentos
from lancamento
group by cod_contrato, cod_contrato_dv, dt_competencia
