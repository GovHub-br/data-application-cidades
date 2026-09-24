{{ config(materialized="table") }}

-- Prata: Desembolsos do FGTS por contrato e mês de referência.
-- Fonte: bronze_sftp_canalfgts_desembolsos
-- Grão: uma linha por contrato e competência de referência.
--
-- `-0` aparece com frequência na origem e é zero, não ausência: fica 0 depois do
-- cast, e não nulo.
select
    trim(cod_contrato) as cod_contrato,
    nullif(trim(cod_contrato_dv), '') as cod_contrato_dv,
    nullif(trim(dte_ano), '') as ano_referencia,
    nullif(trim(dte_mes_ref), '') as mes_referencia,
    {{ parse_competencia("dte_ano || dte_mes_ref") }} as dt_competencia,
    {{ parse_numeric("vlr_liberado") }} as vr_liberado,

    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_sftp_canalfgts_desembolsos") }}
