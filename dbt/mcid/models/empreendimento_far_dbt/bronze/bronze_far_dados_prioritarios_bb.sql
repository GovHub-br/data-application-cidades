{{ config(materialized="table") }}

-- Bronze: Dados Prioritários BB — snapshot consolidado do Banco do Brasil (frente FAR)
-- Fonte: mcmv_staging.dados_prioritarios_recebidos_bb_empreendimentos
-- (staging/sharepoint)
-- Espelho de bronze_far_dados_prioritarios_caixa para o agente BB. Filtra modalidade FAR.
with
    prioritarios_raw as (
        select
            nullif(trim(apf), '') as apf,
            nullif(trim(agente_financeiro), '') as agente_financeiro,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,

            nullif(trim(uf), '') as uf,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge,

            nullif(trim(modalidade), '') as modalidade,
            nullif(trim(situacao_do_empreendimento), '') as situacao,
            nullif(
                trim(detalhamento_da_situacao_do_empreendimento), ''
            ) as situacao_detalhamento,

            {{ parse_numeric('percentual_exec', 'numeric(6, 2)') }} as pct_execucao,

            {{ parse_hist_numeric('valor_contratado') }} as valor_contratado,
            {{ parse_hist_numeric('valor_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_hist_numeric('valor_desembolsado') }} as valor_desembolsado,

            {{ parse_int('uh_contratadas') }} as uh_contratadas,
            {{ parse_int('uh_entregues') }} as uh_entregues,
            {{ parse_int('uh_vigentes') }} as uh_vigentes,

            {{ parse_date_br('data_de_contratacao') }} as dt_contratacao,
            {{ parse_date_br('data_da_previsao_da_entrega') }} as dt_previsao_entrega,
            {{ parse_date_br('data_de_movimento') }} as dt_movimento,

            nullif(trim(logradouro_do_imovel), '') as logradouro,
            nullif(trim(bairro_do_imovel), '') as bairro,
            nullif(trim(cep_do_imovel), '') as cep,
            {{ parse_numeric('latitude_do_imovel', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude_do_imovel', 'numeric(12, 8)') }} as longitude,

            {{ parse_hist_numeric('valor_desembolsado_do_ano_de_referencia') }}
            as valor_desembolsado_ano,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,
            {{ parse_int('quantidade_de_uhs_distratadas') }} as uh_distratadas,
            nullif(trim(observacoes), '') as observacoes,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from
            {{ source("mcmv_staging", "dados_prioritarios_recebidos_bb_empreendimentos") }}
        where trim(modalidade) = 'FAR'
    )

select *
from prioritarios_raw
