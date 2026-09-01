{{ config(materialized="table") }}

-- Silver: Dados Prioritários Banco do Brasil (Rural)
-- Fonte: bronze.dados_prioritarios_recebidos_bb_empreendimentos (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados prioritários do BB limpos, tipados e filtrados para modalidade Rural

-- ATENÇÃO aos nomes de coluna: municapio, situaaao, ca3digo, ima3vel, observaaaes,
-- referaancia, idata_de_movimento. NÃO são erros de digitação deste model — são os
-- nomes REAIS em bronze.dados_prioritarios_recebidos_bb_empreendimentos.
-- O arquivo do BB foi lido com o encoding errado no raw_para_staging.py: os bytes UTF-8
-- foram decodificados como latin-1 antes da normalização do cabeçalho, então "município"
-- virou "municapio" (í -> a), "código" virou "ca3digo" (ó -> a3), "situação" virou
-- "situaaao" (ç e ã -> a). O arquivo equivalente da CAIXA veio limpo, então é específico
-- deste arquivo.
-- Isto é um CONTORNO. O certo é corrigir a detecção de encoding na ingestão e recarregar;
-- quando isso acontecer, este model quebra e os nomes devem voltar ao normal.
with
    prioritarios_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(apf) as apf,
            nullif(trim(agente_financeiro), '') as agente_financeiro,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,
            nullif(trim(modalidade), '') as modalidade,

            -- Localização
            nullif(trim(uf), '') as uf,
            nullif(trim(municapio), '') as municipio,
            nullif(trim(ca3digo_ibge_do_municapio), '') as cod_ibge,

            -- Situação
            nullif(trim(situaaao_do_empreendimento), '') as situacao,
            nullif(trim(detalhamento_da_situaaao_do_empreendimento), '') as situacao_detalhamento,

            -- Execução física (%)
            {{ parse_numeric('"exec"', 'numeric(6, 2)') }} as percentual_execucao_fisica,

            -- Valores
            {{ parse_financial_value('valor_contratado') }} as valor_contratado,
            {{ parse_financial_value('valor_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_desembolsado') }} as valor_desembolsado,
            {{ parse_financial_value('valor_desembolsado_do_ano_de_referaancia') }} as valor_desembolsado_ano,

            -- UHs
            {{ parse_int('uh_contratadas') }} as uh_contratadas,
            {{ parse_int('uh_entregues') }} as uh_entregues,
            {{ parse_int('uh_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,
            {{ parse_int('quantidade_de_uhs_distratadas') }} as uh_distratadas,

            -- Endereço
            nullif(trim(logradouro_do_ima3vel), '') as logradouro,
            nullif(trim(bairro_do_ima3vel), '') as bairro,
            nullif(trim(cep_do_ima3vel), '') as cep,

            -- Coordenadas
            {{ parse_numeric('latitude_do_ima3vel', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude_do_ima3vel', 'numeric(12, 8)') }} as longitude,

            -- Datas
            case
                when data_de_contrataaao is null or trim(data_de_contrataaao) = '' then null
                when data_de_contrataaao ~ '^\d{4}-\d{2}-\d{2}' then data_de_contrataaao::date
                else {{ target.schema }}.parse_date_br(data_de_contrataaao)
            end as dt_contratacao,
            case
                when data_da_previsao_da_entrega is null or trim(data_da_previsao_da_entrega) = '' then null
                when data_da_previsao_da_entrega ~ '^\d{4}-\d{2}-\d{2}' then data_da_previsao_da_entrega::date
                else {{ target.schema }}.parse_date_br(data_da_previsao_da_entrega)
            end as dt_previsao_entrega,
            case
                when idata_de_movimento is null or trim(idata_de_movimento) = '' then null
                when idata_de_movimento ~ '^\d{4}-\d{2}-\d{2}' then idata_de_movimento::date
                else {{ target.schema }}.parse_date_br(idata_de_movimento)
            end as dt_movimento,

            -- Observações
            nullif(trim(observaaaes), '') as observacoes,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ source("staging_lake", "dados_prioritarios_recebidos_bb_empreendimentos") }}
        where trim(upper(modalidade)) = 'RURAL'
    )

select *
from prioritarios_raw
