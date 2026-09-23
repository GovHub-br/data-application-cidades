{{ config(materialized="table") }}

-- Prata: Dados Prioritários SNH (FNHIS)
-- Fonte: bronze_shpt_dados_prioritarios_snh_empreendimentos (a mesma bronze do Rural, que traz
-- todas as modalidades), filtrada para `modalidade = 'FNHIS'`. São os contratos do Sub-50 que a
-- SNH acompanha como empreendimento: 1.224 em 2026-09, todos CAIXA, Novo MCMV.
--
-- Diferenças para o Rural, e por isso este model não reaproveita o dele:
-- - o código do agente é a OPERAÇÃO da CAIXA (7 dígitos), não um APF: `normalize_apf` o
--   deformaria;
-- - o IBGE vem com 6 dígitos (sem verificador), e fica assim em `cod_ibge_6`;
-- - construtora, endereço e coordenadas vêm vazios — é obra pública ainda não iniciada.

select
    -- Identificação
    nullif(trim(identificador_da_operacao_na_snh), '') as id_operacao_snh,
    nullif(trim(codigo_da_operacao_no_agente_financeiro), '') as cod_operacao_agente,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_agente_financeiro)), '') as agente_financeiro,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_do_empreendimento)), '') as empreendimento_nome,
    nullif(trim({{ target.schema }}.corrigir_mojibake(modalidade)), '') as modalidade,
    case when trim(novo_mcmv_sim_nao) = 'Sim' then true else false end as ic_novo_mcmv,

    -- Localização
    upper(nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '')) as municipio,
    nullif(trim(sigla_da_uf), '') as uf,
    nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_uf)), '') as estado_nome,
    nullif(nullif(trim({{ target.schema }}.corrigir_mojibake(nome_da_regiao)), ''), '#N/D') as regiao,
    nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge_6,

    -- Situação
    nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_do_empreendimento)), '') as situacao,
    nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_da_empreendimento_agrupada)), '') as situacao_agrupada,
    case when trim(operacao_vigente_sim_nao) = 'Sim' then true else false end as ic_operacao_vigente,

    -- Execução física (%)
    {{ parse_numeric('percentual_da_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,

    -- UHs
    {{ parse_int('unidades_contratadas') }} as uh_contratadas,
    {{ parse_int('unidades_entregues') }} as uh_entregues,
    {{ parse_int('unidades_vigentes') }} as uh_vigentes,
    {{ parse_int('unidades_distratadas') }} as uh_distratadas,
    {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

    -- Valores
    {{ parse_financial_value('valor_contratado_original') }} as valor_contratado_original,
    {{ parse_financial_value('valor_do_aporte_adicional') }} as valor_aporte_adicional,
    {{ parse_financial_value('valor_contratado_total') }} as valor_contratado,
    {{ parse_financial_value('valor_desembolsado_total') }} as valor_desembolsado,

    -- Datas
    {{ target.schema }}.parse_date_br(nullif(trim(data_da_contratacao), '')) as dt_contratacao,
    {{ target.schema }}.parse_date_br(nullif(trim(data_de_previsao_de_termino), '')) as dt_previsao_termino,
    {{ target.schema }}.parse_date_br(nullif(trim(data_do_termino), '')) as dt_termino,
    {{ target.schema }}.parse_date_br(nullif(trim(data_de_referencia), '')) as dt_referencia,

    -- Linhagem
    _source_file as arquivo_de_origem,
    nullif(trim(_ingested_at), '')::timestamp as criado_em

from {{ ref("bronze_shpt_dados_prioritarios_snh_empreendimentos") }}
where trim(upper(modalidade)) = 'FNHIS'
