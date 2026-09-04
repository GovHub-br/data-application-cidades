{{ config(materialized="table") }}

-- Bronze: Dados Prioritários SNH — snapshot corrente (frente Rural / PNHR)
-- Fonte: mcmv_staging.dados_prioritarios_disponibilizados_snh_empreendimentos
-- (snapshot único 30/09/2025; NÃO é a série mensal).
-- Recorte: upper(modalidade) = 'RURAL' (cobre 'Rural' e 'RURAL'). Chave:
-- normalize_apf(codigo_da_operacao_no_agente_financeiro). 127/127 casam com o cadastro
-- PJ.
with
    snh_raw as (
        select
            {{ normalize_apf('codigo_da_operacao_no_agente_financeiro') }} as apf,
            nullif(trim(identificador_da_operacao_na_snh), '') as id_operacao_snh,
            nullif(trim(nome_do_agente_financeiro), '') as agente_financeiro,
            nullif(trim(nome_da_construtora_entidade), '') as construtora_entidade_nome,
            nullif(trim(nome_do_empreendimento), '') as empreendimento_nome,

            nullif(trim(sigla_da_uf), '') as uf,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge,
            nullif(trim(nome_da_regiao), '') as regiao,

            nullif(trim(modalidade), '') as modalidade,
            nullif(trim(situacao_do_empreendimento), '') as situacao,
            nullif(
                trim(detalhamento_da_situacao_do_empreendimento), ''
            ) as situacao_detalhamento,
            nullif(trim(situacao_da_empreendimento_agrupada), '') as situacao_agrupada,

            {{ parse_numeric('percentual_da_obra', 'numeric(6, 2)') }} as pct_execucao,

            {{ parse_hist_numeric('valor_contratado_total') }} as valor_contratado_total,
            {{ parse_hist_numeric('valor_do_aporte_adicional') }}
            as valor_aporte_adicional,
            {{ parse_hist_numeric('valor_desembolsado_total') }} as valor_desembolsado,

            {{ parse_int('unidades_contratadas') }} as uh_contratadas,
            {{ parse_int('unidades_entregues') }} as uh_entregues,
            {{ parse_int('unidades_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_distratadas') }} as uh_distratadas,

            {{ parse_date_br('data_da_contratacao') }} as dt_contratacao,
            {{ parse_date_br('data_do_termino') }} as dt_termino,
            {{ parse_date_br('data_de_previsao_de_termino') }} as dt_previsao_termino,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ parse_date_br('data_de_referencia') }} as dt_referencia

        from
            {{ source("mcmv_staging", "dados_prioritarios_disponibilizados_snh_empreendimentos") }}
        where upper(trim(modalidade)) = 'RURAL'
    )

select *
from snh_raw
