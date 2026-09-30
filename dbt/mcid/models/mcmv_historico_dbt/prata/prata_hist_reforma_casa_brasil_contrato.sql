{{ config(materialized="table") }}

-- SILVER — contratos Reforma Casa Brasil, a partir de
-- bronze_sftp_reforma_casa_brasil (snapshots semanais GEFUS). Mesmo desenho
-- de prata_hist_classe_media_contrato (D3 da change
-- frentes-restantes-mcmv-historico — schemas quase idênticos): tipagem +
-- dedup por (nu_contrato, dt_referencia).
--
-- Diferença de schema: a coluna de tipo de desembolso chega com 2 grafias
-- conforme a semana (`tipo_desemblso`, herdado do mesmo erro de digitação da
-- fonte de Classe Média; `tipo_desembolso`, grafia correta em semanas mais
-- recentes) — `coalesce` das duas. `nu_dias_atraso` /
-- `no_endrco_crspndca` / `no_bairro_crspndca` só existem em semanas mais
-- recentes (drift de schema; ausência é `null`, não bug).
--
-- PII de mutuário (D4): as 3 colunas de `macros/historico/pii_mutuario.sql`
-- existem na bronze mas NÃO são projetadas aqui — conferido pelo teste
-- `pii_mutuario_ausente` no schema.yml.
{% set bronze = ref('bronze_sftp_reforma_casa_brasil') %}

with

    tipado as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'Reforma Casa Brasil'::text as frente_mcmv,
            'Financiada'::text as grupo_linha,
            'Reforma Casa Brasil'::text as linha_mcmv,
            nullif(trim(nu_contrato), '')::text as nu_contrato,
            nullif(trim(nu_contrato_repasse), '')::text as nu_contrato_repasse,
            nullif(trim(nu_contrato_passivo), '')::text as nu_contrato_passivo,
            nullif(trim(sg_sexo), '')::text as sg_sexo,
            nullif(trim(nu_pis), '')::text as nu_pis,
            {{ parse_hist_numeric('vr_renda_familiar_comprovada') }}
            as valor_renda_familiar_comprovada,
            {{ parse_hist_double('pc_renda_informal') }} as percentual_renda_informal,
            nullif(trim(ed_bairro_imovel_garantia), '')::text as bairro_imovel,
            nullif(trim(sg_uf_imovel), '')::text as uf,
            nullif(trim(no_municipio_imovel), '')::text as municipio,
            nullif(trim(ed_cep_imovel_garantia), '')::text as cep_imovel,
            nullif(trim(co_municipio_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(co_classificacao_imovel), '')::text as classificacao_imovel,
            {{ parse_hist_double('nu_area_imovel') }} as area_imovel,
            nullif(trim(linha_apf), '')::text as linha_apf,
            nullif(trim(modalidade), '')::text as modalidade,
            {{ parse_date_br('dt_evento') }} as dt_contratacao,
            nullif(trim(ano_orcamento), '')::text as ano_orcamento,
            {{ parse_hist_numeric('vr_avaliacao_terreno') }} as valor_avaliacao_terreno,
            {{ parse_hist_numeric('vr_evento') }} as valor_evento,
            {{ parse_hist_numeric('vr_desconto_resolucao_460') }}
            as valor_desconto_resolucao_460,
            {{ parse_hist_numeric('vr_recurso_proprio') }} as valor_recurso_proprio,
            {{ parse_hist_numeric('vr_fgts_utilizado') }} as valor_fgts_utilizado,
            nullif(trim(coalesce(tipo_desembolso, tipo_desemblso)), '')::text
            as tipo_desembolso,
            {{ parse_hist_double('pc_taxa_juros_nominal_inicial') }}
            as percentual_taxa_juros_nominal_inicial,
            nullif(trim(nu_tipo_garantia), '')::text as tipo_garantia,
            {{ parse_hist_numeric('vr_garantia') }} as valor_garantia,
            nullif(trim(situacao_garantia), '')::text as situacao_garantia,
            {{ parse_hist_bigint('pz_financiamento') }} as prazo_financiamento_meses,
            nullif(trim(no_sistema_amortizacao), '')::text as sistema_amortizacao,
            {{ parse_hist_numeric('vr_prestacao_inicial') }} as valor_prestacao_inicial,
            {{ parse_hist_numeric('vr_pagamento_amortizacao') }}
            as valor_pagamento_amortizacao,
            {{ parse_hist_numeric('vr_pagamento_juros') }} as valor_pagamento_juros,
            nullif(trim(nu_unidade_operacional), '')::text as unidade_operacional,
            nullif(trim(nu_codigo_atu), '')::text as codigo_empreendimento,
            nullif(trim(nu_produto), '')::text as produto,
            nullif(trim(tipo_imovel), '')::text as tipo_imovel,
            nullif(trim(faixa_renda), '')::text as faixa_renda,
            nullif(trim(ic_cotista), '')::text as indicador_cotista,
            nullif(trim(nu_legislacao), '')::text as legislacao,
            {{ parse_hist_numeric('vr_investimento') }} as valor_contratado,
            {{ parse_hist_bigint('nu_dias_atraso') }} as dias_atraso,
            nullif(trim(no_endrco_crspndca), '')::text as endereco_correspondencia,
            nullif(trim(no_bairro_crspndca), '')::text as bairro_correspondencia,
            dt_referencia,
            source_file,
            hash_linha,
            dt_ingest
        from {{ bronze }}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by nu_contrato, dt_referencia order by source_file desc
            ) as rn
        from tipado
    )

select
    programa,
    frente_mcmv,
    grupo_linha,
    linha_mcmv,
    nu_contrato,
    nu_contrato_repasse,
    nu_contrato_passivo,
    sg_sexo,
    nu_pis,
    valor_renda_familiar_comprovada,
    percentual_renda_informal,
    bairro_imovel,
    uf,
    municipio,
    cep_imovel,
    codigo_ibge_municipio,
    classificacao_imovel,
    area_imovel,
    linha_apf,
    modalidade,
    dt_contratacao,
    ano_orcamento,
    valor_avaliacao_terreno,
    valor_evento,
    valor_desconto_resolucao_460,
    valor_recurso_proprio,
    valor_fgts_utilizado,
    tipo_desembolso,
    percentual_taxa_juros_nominal_inicial,
    tipo_garantia,
    valor_garantia,
    situacao_garantia,
    prazo_financiamento_meses,
    sistema_amortizacao,
    valor_prestacao_inicial,
    valor_pagamento_amortizacao,
    valor_pagamento_juros,
    unidade_operacional,
    codigo_empreendimento,
    produto,
    tipo_imovel,
    faixa_renda,
    indicador_cotista,
    legislacao,
    valor_contratado,
    dias_atraso,
    endereco_correspondencia,
    bairro_correspondencia,
    dt_referencia,
    source_file,
    hash_linha,
    dt_ingest
from dedup
where rn = 1 and nu_contrato is not null
