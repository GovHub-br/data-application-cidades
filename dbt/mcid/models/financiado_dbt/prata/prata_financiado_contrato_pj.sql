{{ config(materialized="table") }}

-- Prata: Contrato PJ do Canal FGTS, com os domínios já resolvidos em texto.
-- Fonte: bronze_sftp_canalfgts_contratos + domínios (linha, modalidade, situação, área)
-- Grão: uma linha por `cod_contrato`.
--
-- `ic_apoio_producao` marca a linha `33` (Apoio à Produção), que é a que sustenta a
-- MCMV-Financiada e sobre a qual o MCMV Cidades aporta. As demais linhas do pacote
-- (saneamento, pró-transporte, pró-moradia) ficam na tabela de propósito: são o mesmo
-- contrato no mesmo sistema, e filtrá-las aqui esconderia o denominador.
with
    contrato as (
        select
            trim(cod_contrato) as cod_contrato,
            nullif(trim(cod_contrato_dv), '') as cod_contrato_dv,
            trim(cod_empreendimento) as cod_empreendimento,

            -- O APF chega PARTIDO nesta origem: `cod_contrato` é o número e
            -- `cod_contrato_dv` é o dígito verificador. Concatenados formam o mesmo
            -- APF de 9 dígitos que o `Base_PJ_FGTS` e o FAR usam inteiro, e é assim
            -- que este domínio se liga ao resto do MCMV.
            nullif(trim(cod_contrato), '') || nullif(trim(cod_contrato_dv), '')
            as nu_apf,

            -- Na origem o par operação/AF repete o próprio contrato na maioria das
            -- linhas, mas é ele que o lado PF cita como `operacao`.
            nullif(trim(cod_operacao_agente_financeiro), '') as cod_operacao,
            nullif(trim(cod_operacao_agente_financeiro_dv), '') as cod_operacao_dv,

            nullif(trim(cod_linha), '') as cod_linha,
            nullif(trim(cod_modalidade), '') as cod_modalidade,
            nullif(trim(cod_area), '') as cod_area,
            nullif(trim(cod_situacao_contrato), '') as cod_situacao_contrato,
            nullif(trim(cod_tomador), '') as cod_tomador,
            nullif(trim(cod_af), '') as cod_agente_financeiro,
            nullif(trim(cod_objetivo), '') as cod_objetivo,
            nullif(trim(uf), '') as uf,
            nullif(trim(txt_eixo), '') as eixo,

            {{ parse_numeric("vlr_contratado") }} as vr_contratado,
            {{ parse_numeric("vlr_investimento") }} as vr_investimento,
            {{ parse_data_canal_fgts("dte_assinatura") }} as dt_assinatura,
            {{ parse_int("dte_orcamento_ano") }} as ano_orcamento,

            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_canalfgts_contratos") }}
    ),

    linha as (
        select trim(codigo) as cod, nullif(trim(linha), '') as nome
        from {{ ref("bronze_sftp_canalfgts_linha") }}
    ),

    modalidade as (
        select trim(codigo) as cod, nullif(trim(modalidade), '') as nome
        from {{ ref("bronze_sftp_canalfgts_modalidade") }}
    ),

    area as (
        select trim(codigo) as cod, nullif(trim(area), '') as nome
        from {{ ref("bronze_sftp_canalfgts_area") }}
    ),

    situacao as (
        select trim(codigo) as cod, nullif(trim(situacaodocontrato), '') as nome
        from {{ ref("bronze_sftp_canalfgts_situacao_contrato") }}
    ),

    tomador as (
        select
            trim(codigo) as cod,
            nullif(trim(entidade), '') as nome,
            nullif(regexp_replace(trim(cgc), '[^0-9]', '', 'g'), '') as cnpj
        from {{ ref("bronze_sftp_canalfgts_entidades") }}
    ),

    -- Classificação da origem por linha + objetivo. ATENÇÃO ao que ela significa: é
    -- o OBJETIVO ORÇAMENTÁRIO da operação, não a natureza jurídica do tomador.
    -- Nenhum contrato desta tabela é firmado com pessoa física — o lado "PF" é um
    -- agregado do financiamento aos adquirentes, lançado sob um tomador sintético
    -- ("ADQUIRENTE FINAL PESSOA FISICA..."). Contrato de mutuário individual mora no
    -- CCI/CCA, em `prata_financiado_contrato_pf`.
    -- Fora do Apoio à Produção a origem não classifica, e a coluna fica nula.
    pj_pf as (
        select
            trim(cod_linha) as cod_linha,
            trim(cod_objetivo) as cod_objetivo,
            nullif(trim(tipo), '') as tipo
        from {{ ref("bronze_sftp_canalfgts_operacoes_pj_pf") }}
    )

select
    c.cod_contrato,
    c.cod_contrato_dv,
    c.nu_apf,
    {{ var('schema_udfs') }}.normalize_apf(c.nu_apf) as apf,
    c.cod_empreendimento,
    c.cod_operacao,
    c.cod_operacao_dv,
    c.cod_linha,
    l.nome as linha,
    c.cod_modalidade,
    md.nome as modalidade,
    c.cod_area,
    a.nome as area,
    c.cod_situacao_contrato,
    s.nome as situacao_contrato,
    c.cod_tomador,
    t.nome as tomador_nome,
    t.cnpj as tomador_cnpj,
    c.cod_agente_financeiro,
    c.cod_objetivo,
    p.tipo as tipo_operacao,
    c.uf,
    c.eixo,
    c.vr_contratado,
    c.vr_investimento,
    c.dt_assinatura,
    c.ano_orcamento,
    c.cod_linha = '33' as ic_apoio_producao,

    -- Contrato ÂNCORA do empreendimento: aquele cujo código é o próprio código do
    -- empreendimento. É o recorte que faz "um APF = um empreendimento" valer.
    --
    -- Esta tabela é o razão de operações da CAIXA, não um cadastro de
    -- empreendimentos: sob o mesmo `cod_empreendimento` convivem a operação que
    -- financia a construtora e as que agregam o financiamento aos adquirentes, cada
    -- uma com APF próprio. O inverso nunca ocorre — todo contrato pertence a um só
    -- empreendimento. O `Base_PJ_FGTS` segue apenas o âncora, e somar por
    -- `cod_empreendimento` sem este filtro mistura os dois lados e infla o total.
    --
    -- O âncora é o discriminador EXATO do recorte acompanhado; `tipo_operacao` = 'PJ'
    -- chega perto mas não fecha, porque há âncora classificado como PF.
    c.cod_contrato = c.cod_empreendimento as ic_contrato_ancora,
    c.cod_situacao_contrato = '00' as ic_situacao_normal,
    c.arquivo_de_origem,
    c.criado_em
from contrato as c
left join linha as l on c.cod_linha = l.cod
left join modalidade as md on c.cod_modalidade = md.cod
left join area as a on c.cod_area = a.cod
left join situacao as s on c.cod_situacao_contrato = s.cod
left join tomador as t on c.cod_tomador = t.cod
left join pj_pf as p on c.cod_linha = p.cod_linha and c.cod_objetivo = p.cod_objetivo
