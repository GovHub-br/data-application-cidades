{{ config(materialized="table") }}

-- Prata: Posição do empreendimento — datas de início, término e inauguração.
-- Fonte: bronze_sftp_canalfgts_empreendimentos_posicoes
-- Grão: uma linha por `cod_empreendimento`.
--
-- Complementa `prata_financiado_obra_posicao`, que é por CONTRATO: aqui estão as
-- datas do empreendimento inteiro, inclusive a de inauguração, que a execução de
-- obra não registra.
select
    trim(cod_empreendimento) as cod_empreendimento,
    trim(dte_ano_mes) as competencia,
    {{ parse_competencia("dte_ano_mes") }} as dt_competencia,
    {{ parse_numeric("prc_obra_executada_ult", "numeric(9, 4)") }} as pct_obra_executada,
    {{ parse_data_canal_fgts("dtinicio") }} as dt_inicio,
    {{ parse_data_canal_fgts("dttermino") }} as dt_termino,
    {{ parse_data_canal_fgts("dtinauguracao") }} as dt_inauguracao,

    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_sftp_canalfgts_empreendimentos_posicoes") }}
