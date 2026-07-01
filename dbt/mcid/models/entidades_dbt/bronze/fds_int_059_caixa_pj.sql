{{ config(materialized="table") }}

-- Bronze: INT 059 — empreendimentos FDS da CAIXA (complementar)
-- Fonte: int_empreendimentos_int_059_fds_caixa_pj
-- OBS: Esta tabela contém TANTO legado (PMCMV-E) quanto Novo MCMV.
--      Filtrar por no_selecao_pmcmv_e = 'NOVO PMCMV-E' para manter apenas Novo MCMV (305 registros).
-- Campos exclusivos não presentes nas fontes primárias:
--   situacao_gefus, fase_contrato, situacao_empreendimento, no_selecao_pmcmv_e

with
    int059_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim(no_empreeendmento), '') as empreendimento_nome,

            -- Seleção MCMV (campo-chave para filtrar Novo vs Legado)
            nullif(trim(no_selecao_pmcmv_e), '') as selecao_pmcmv_e,

            -- Status exclusivos (não disponíveis nas fontes primárias)
            nullif(trim(situacao_gefus), '') as situacao_gefus,
            nullif(trim(fase_contrato), '') as fase_contrato,
            nullif(trim(situacao_empreendimento), '') as situacao_empreendimento,

            -- Proponente (Entidade Organizadora)
            nullif(trim(razao_social_proponente), '') as eo_nome,
            nullif(regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'), '') as eo_cnpj,
            nullif(trim(email_entidade), '') as eo_email,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(cod_municipio_ibge), '') as cod_ibge,
            nullif(trim(no_regiao), '') as regiao,

            -- UHs
            {{ parse_int('qt_unidade_financiadas') }} as qt_uh_financiadas,
            {{ parse_int('qt_unidades_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_uh_entregues,
            {{ parse_int('quantidade_uh_adaptadas') }} as qt_uh_adaptadas,

            -- Execução física
            {{ parse_numeric('percentual_obra_realizado', 'numeric(6, 2)') }} as pct_obra_realizado,

            -- Valores financeiros
            {{ parse_financial_value('vr_emprestimo_original') }} as vr_emprestimo_original,
            {{ parse_financial_value('vlr_operacao') }} as vr_operacao,
            {{ parse_financial_value('vr_investimento') }} as vr_investimento,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_liberado_sisfin') }} as vr_liberado_sisfin,
            {{ parse_financial_value('vr_projeto') }} as vr_projeto,
            {{ parse_financial_value('vr_obra') }} as vr_obra,
            {{ parse_financial_value('valor_aporte_adicional') }} as vr_aporte_adicional,
            {{ parse_financial_value('valor_aporte_adicional_contratado') }} as vr_aporte_contratado,

            -- Tipologia
            nullif(trim(dsc_tipologia), '') as tipologia,
            nullif(trim(cod_regime_execucao), '') as co_regime_execucao,
            nullif(trim(regime_construcao), '') as regime_construcao,
            nullif(trim(modalidade_requalificacao), '') as modalidade_requalificacao,
            nullif(trim(tipo_de_unidade_do_empreendimento), '') as tipo_unidade,

            -- Terreno e indicadores
            nullif(trim(terreno_doado), '') as terreno_doado,
            nullif(trim(tipo_terreno), '') as tipo_terreno,
            nullif(trim(aquecimento_solar), '') as aquecimento_solar,
            nullif(trim(infraestrutura_externa), '') as infraestrutura_externa,

            -- Datas-chave
            {{ target.schema }}.parse_date_br(dt_assinatura) as dt_contratacao,
            {{ target.schema }}.parse_date_br(dt_inicio_obra) as dt_inicio_obra,
            {{ target.schema }}.parse_date_br(dt_termino_obra) as dt_termino_obra,
            {{ target.schema }}.parse_date_br(dt_legalizacao) as dt_legalizacao,
            {{ target.schema }}.parse_date_br(dt_maxima_liberacao) as dt_maxima_liberacao,
            {{ target.schema }}.parse_date_br(dt_ultima_entrega) as dt_ultima_entrega,
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,

            -- Coordenadas GPS
            {{ parse_numeric('gps_latitude_grau') }} as gps_lat_grau,
            {{ parse_numeric('gps_latitude_minuto') }} as gps_lat_minuto,
            {{ parse_numeric('gps_latitude_segundo') }} as gps_lat_segundo,
            {{ parse_numeric('gps_longitude_grau') }} as gps_long_grau,
            {{ parse_numeric('gps_longitude_minuto') }} as gps_long_minuto,
            {{ parse_numeric('gps_longitude_segundo') }} as gps_long_segundo,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "int_empreendimentos_int_059_fds_caixa_pj") }}
    )

select *
from int059_raw
