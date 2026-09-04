{{ config(materialized="table") }}

with
    fonte as (
        select
            ano::integer as ano,
            uh_ogu_subsidiado::integer as uh_ogu_subsidiado,
            uh_fgts_financiado::integer as uh_fgts_financiado,
            snapshot_date::date as snapshot_date,
            source_file::text as source_file
        from {{ ref("issue_118_mcmv_serie_temporal_piloto") }}
    ),

    serie_longa as (
        select
            ano,
            'OGU/Subsidiado'::text as linha_historica,
            'Subsidiada'::text as grupo_linha,
            uh_ogu_subsidiado as quantidade_uh,
            'dados_abertos_mcmv_ogu_empreendimentos'::text as fonte_primaria,
            snapshot_date,
            source_file
        from fonte

        union all

        select
            ano,
            'FGTS/Financiado'::text as linha_historica,
            'Financiada'::text as grupo_linha,
            uh_fgts_financiado as quantidade_uh,
            'dados_abertos_mcmv_fgts_sintetico'::text as fonte_primaria,
            snapshot_date,
            source_file
        from fonte
    )

select
    md5(
        concat_ws(
            '|', 'mcmv-serie-temporal', linha_historica, ano::text, snapshot_date::text
        )
    ) as id_historico_snapshot,
    md5(
        concat_ws('|', 'mcmv-serie-temporal', linha_historica, ano::text)
    ) as id_negocio_historico,
    'Minha Casa Minha Vida'::text as programa,
    linha_historica,
    grupo_linha,
    'ano'::text as granularidade_temporal,
    ano,
    make_date(ano, 12, 31) as dt_referencia,
    quantidade_uh,
    fonte_primaria,
    source_file,
    'raw/dados_historicos + staging'::text as source_path,
    snapshot_date,
    md5(
        concat_ws('|', ano::text, linha_historica, quantidade_uh::text, fonte_primaria)
    ) as hash_linha,
    snapshot_date::timestamp as dt_ingest,
    snapshot_date::timestamp as dt_valid_from,
    null::timestamp as dt_valid_to,
    true as is_current,
    'snapshot_completo_anual'::text as estrategia_versionamento,
    'retencao_indeterminada_para_auditoria_e_backtest'::text as regra_retencao,
    'Piloto issue #118: serie historica anual usada para consultas temporais, backtest do relogio e validacao de reprocessamento.'
    ::text as observacao_historico
from serie_longa
