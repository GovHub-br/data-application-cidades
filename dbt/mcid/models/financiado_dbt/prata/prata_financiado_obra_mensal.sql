{{ config(materialized="table") }}

-- Prata: Execução física do contrato, mês a mês.
-- Fonte: bronze_sftp_canalfgts_execucoes_obras + bronze_sftp_canalfgts_situacao_obra
-- Grão: uma linha por contrato e competência.
--
-- A tabela da origem mistura DUAS coisas sob o mesmo layout: medição realizada e
-- cronograma futuro. O que separa as duas é `cod_situacao_obra`: linha com situação
-- em branco é cronograma, e linha medida nunca está no futuro. Por isso `ic_medido`
-- existe e é a trava de toda comparação — sem ela, uma competência ainda por vir
-- entra na conta com realizado zero e vira atraso de cem pontos.
with
    execucao as (
        select
            trim(cod_contrato) as cod_contrato,
            trim(dte_ano_mes_avaliacao) as competencia,
            {{ parse_competencia("dte_ano_mes_avaliacao") }} as dt_competencia,
            nullif(trim(cod_situacao_obra), '') as cod_situacao_obra,
            nullif(trim(cod_execucao_obra), '') as cod_execucao_obra,
            {{ parse_numeric("prc_prev_acum_mes", "numeric(9, 4)") }} as pct_previsto,
            {{ parse_numeric("prc_real_acum_mes", "numeric(9, 4)") }} as pct_realizado,
            {{ parse_data_canal_fgts("dte_ultima_vistoria") }} as dt_ultima_vistoria,
            nullif(trim(txt_providencias), '') as providencias,

            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_canalfgts_execucoes_obras") }}
    ),

    situacao as (
        select trim(codigo) as cod, nullif(trim(situacaodaobra), '') as nome
        from {{ ref("bronze_sftp_canalfgts_situacao_obra") }}
    )

select
    e.cod_contrato,
    e.competencia,
    e.dt_competencia,
    e.cod_situacao_obra,
    s.nome as situacao_obra,
    e.cod_execucao_obra,
    e.pct_previsto,
    e.pct_realizado,
    e.pct_previsto - e.pct_realizado as pct_desvio,
    e.dt_ultima_vistoria,
    e.providencias,

    -- Linha de medição real; falso quando é apenas cronograma previsto.
    e.cod_situacao_obra is not null as ic_medido,

    -- Competência ilegível na origem, como `000000`. Fica marcada em vez de
    -- descartada: sumir com a linha esconderia o defeito do arquivo.
    e.dt_competencia is null as ic_competencia_invalida,

    e.arquivo_de_origem,
    e.criado_em
from execucao as e
left join situacao as s on e.cod_situacao_obra = s.cod
