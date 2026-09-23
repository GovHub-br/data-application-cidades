{{ config(materialized="table") }}

-- Prata: Contrato do Pró-Moradia — visão unificada
-- Fonte: bronze_shpt_fgts_canal_contratos, filtrada para `cod_linha = '26'`, enriquecida com o
-- cadastro do empreendimento, a última posição de obra e as tabelas de domínio do Canal FGTS.
--
-- Por que a linha, e não a modalidade: o Canal FGTS traz todos os programas do fundo (Apoio à
-- Produção, Carta de Crédito, Pró-Transporte, Saneamento para Todos...) e o Pró-Moradia não é
-- uma modalidade — é a LINHA `26` (`HAB / PRO-MORADIA` em bronze_shpt_fgts_canal_tdom_linha).
-- A modalidade diz o TIPO de intervenção dentro do programa (urbanização, produção de conjunto,
-- cesta de materiais...). O teste `pro_moradia_linha_26_e_pro_moradia` falha se o código mudar
-- de significado na origem.
--
-- Grão: um contrato. Conferido em 2026-09 sobre a remessa MC20260220: 686 contratos, um por
-- empreendimento, e `cod_contrato` já é único sem o dígito verificador.
--
-- Datas do canal vêm como `MM/DD/YY HH:MI:SS` (padrão americano, ano com dois dígitos): o
-- `parse_date_br` não reconhece esse formato e devolveria NULL em todas.

with
    contrato as (
        select *
        from {{ ref("bronze_shpt_fgts_canal_contratos") }}
        where trim(cod_linha::text) = '26'
    ),

    empreendimento as (
        select * from {{ ref("bronze_shpt_fgts_canal_empreendimentos") }}
    ),

    posicao as (
        select * from {{ ref("bronze_shpt_fgts_canal_empreendimentos_posicoes") }}
    ),

    linha as (
        select trim(codigo::text) as codigo, trim(linha::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_linha") }}
    ),

    modalidade as (
        select trim(codigo::text) as codigo, trim(modalidade::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_modalidade") }}
    ),

    situacao as (
        select trim(codigo::text) as codigo, trim(situacao_do_contrato::text) as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_situacao_contrato") }}
    ),

    entidade as (
        select
            lpad(trim(codigo::text), 5, '0') as codigo,
            nullif(trim(entidade::text), '') as nome,
            nullif(regexp_replace(cgc::text, '[^0-9]', '', 'g'), '') as cnpj
        from {{ ref("bronze_shpt_fgts_canal_tdom_entidades") }}
    ),

    municipio as (
        select
            lpad(trim(codigo::text), 4, '0') as codigo,
            nullif(trim(municipio::text), '') as nome,
            nullif(trim(uf::text), '') as uf,
            nullif(trim(ibge_codigo::text), '') as cod_ibge
        from {{ ref("bronze_shpt_fgts_canal_tdom_municipios") }}
    ),

    uor as (
        select trim(codigo::text) as codigo, nullif(trim(uor::text), '') as nome
        from {{ ref("bronze_shpt_fgts_canal_tdom_uor") }}
    )

select
    -- Identificação
    trim(c.cod_contrato::text) as cod_contrato,
    trim(c.cod_contrato_dv::text) as cod_contrato_dv,
    trim(c.cod_contrato::text) || '-' || trim(c.cod_contrato_dv::text) as contrato,
    trim(c.cod_empreendimento::text) as cod_empreendimento,
    nullif(trim(c.cod_operacao_agente_financeiro::text), '') as cod_operacao_agente_financeiro,

    -- Programa e tipo de intervenção
    trim(c.cod_linha::text) as linha_codigo,
    l.nome as linha,
    trim(c.cod_modalidade::text) as modalidade_codigo,
    m.nome as modalidade,
    -- As quatro frentes da carta-consulta do Pró-Moradia (Desenvolvimento Institucional,
    -- Provisão Habitacional, Provisão de Lote Urbanizado e Urbanização), por código de
    -- modalidade. Lote urbanizado entra em provisão: as modalidades de lote do canal
    -- (014, 019, 082) são mistas com unidade habitacional.
    case
        when trim(c.cod_modalidade::text) in ('002', '003', '070', '078', '218', '981')
            then 'Urbanização'
        when trim(c.cod_modalidade::text) in ('014', '018', '019', '021', '082', '194')
            then 'Provisão habitacional'
        when trim(c.cod_modalidade::text) = '011'
            then 'Desenvolvimento institucional'
        when trim(c.cod_modalidade::text) = '221'
            then 'Situação de emergência'
        else 'Não classificada'
    end as tipo_intervencao,

    -- Empreendimento
    upper(nullif(trim(e.txt_nome_empreendimento::text), '')) as empreendimento_nome,
    nullif(trim(e.txt_projeto::text), '') as projeto,
    nullif(trim(e.txt_objeto::text), '') as objeto,

    -- Tomador: o ente público que contrai o financiamento
    lpad(trim(c.cod_tomador::text), 5, '0') as tomador_codigo,
    t.nome as tomador_nome,
    t.cnpj as tomador_cnpj,
    case
        when t.nome ~* '^(ESTADO|GOVERNO DO ESTADO)' or t.nome ~* '^DISTRITO FEDERAL' then 'Estado/DF'
        when t.nome ~* '^(MUNICIPIO|PREFEITURA)' then 'Município'
        when t.nome is null then null
        else 'Companhia ou autarquia'
    end as tomador_esfera,

    -- Agente financeiro e acompanhamento
    trim(c.cod_af::text) as agente_financeiro_codigo,
    af.nome as agente_financeiro,
    trim(c.cod_uor::text) as uor_codigo,
    u.nome as uor,

    -- Localização: a UF do contrato é a referência; o município vem do empreendimento.
    -- Conferido em 2026-09: nos 678 contratos com município traduzido, as duas UFs batem.
    coalesce(nullif(trim(c.uf::text), ''), mu.uf) as uf,
    mu.nome as municipio,
    mu.cod_ibge,

    -- Situação do contrato
    trim(c.cod_situacao_contrato::text) as situacao_contrato_codigo,
    s.nome as situacao_contrato,
    -- distratado (01, 04), cancelado (02, 10, 21) ou extinto (03): o contrato deixou de
    -- existir. Cláusula suspensiva e liminar seguem vigentes.
    trim(c.cod_situacao_contrato::text) not in ('01', '02', '03', '04', '10', '21') as ic_contrato_vigente,

    -- Datas
    case
        when trim(c.dte_assinatura::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(c.dte_assinatura::text), 8), 'MM/DD/YY')
    end as dt_assinatura,
    {{ parse_int("c.dte_orcamento_ano::text") }} as ano_orcamento,

    -- Valores (o canal usa ponto decimal): o financiamento é o contratado; o investimento
    -- soma a contrapartida do tomador.
    {{ parse_numeric("c.vlr_contratado::text", "numeric(15, 2)") }} as valor_contratado,
    {{ parse_numeric("c.vlr_investimento::text", "numeric(15, 2)") }} as valor_investimento,
    {{ parse_numeric("c.vlr_investimento::text", "numeric(15, 2)") }}
    - {{ parse_numeric("c.vlr_contratado::text", "numeric(15, 2)") }} as valor_contrapartida,

    -- Metas: 0 UH é legítimo em urbanização, que não produz unidade.
    {{ parse_int("e.qtd_unidades_financiadas::text") }} as quantidade_uh_financiadas,
    {{ parse_numeric("e.qtd_populacao_beneficiada::text", "numeric(15, 0)") }} as populacao_beneficiada,
    {{ parse_numeric("e.qtd_empregos_gerados::text", "numeric(15, 0)") }} as empregos_gerados,

    -- PAC e seleção de origem
    trim(c.cod_pac::text) = 'S' as ic_pac,
    nullif(trim(c.cod_ident_externo::text), '') as identificador_selecao,

    -- Última posição de obra do empreendimento (o acompanhamento mensal por contrato está em
    -- prata_pro_moradia_execucao_obra; esta é a foto do cadastro)
    {{ parse_numeric("p.prc_obra_executada_ult::text", "numeric(6, 2)") }} as percentual_obra_posicao,
    case
        when trim(p.dte_ano_mes::text) ~ '^\d{6}$'
        then to_date(trim(p.dte_ano_mes::text) || '01', 'YYYYMMDD')
    end as dt_posicao,
    case
        when trim(p.dt_inicio::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_inicio::text), 8), 'MM/DD/YY')
    end as dt_inicio_obra,
    case
        when trim(p.dt_termino::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_termino::text), 8), 'MM/DD/YY')
    end as dt_termino_obra,
    case
        when trim(p.dt_inauguracao::text) ~ '^\d{2}/\d{2}/\d{2}'
        then to_date(left(trim(p.dt_inauguracao::text), 8), 'MM/DD/YY')
    end as dt_inauguracao,

    -- Linhagem: a remessa do Canal FGTS (MCaaaammdd.zip) de onde veio o contrato
    nullif(trim(c.arquivo_de_origem::text), '') as remessa_canal_fgts,
    to_date(substring(c.arquivo_de_origem::text from 'MC(\d{8})'), 'YYYYMMDD') as dt_remessa,
    nullif(trim(c._ingested_at::text), '')::timestamp as criado_em

from contrato c
left join empreendimento e on trim(e.cod_empreendimento::text) = trim(c.cod_empreendimento::text)
left join posicao p on trim(p.cod_empreendimento::text) = trim(c.cod_empreendimento::text)
left join linha l on l.codigo = trim(c.cod_linha::text)
left join modalidade m on m.codigo = trim(c.cod_modalidade::text)
left join situacao s on s.codigo = trim(c.cod_situacao_contrato::text)
left join entidade t on t.codigo = lpad(trim(c.cod_tomador::text), 5, '0')
left join entidade af on af.codigo = lpad(trim(c.cod_af::text), 5, '0')
left join municipio mu on mu.codigo = lpad(trim(e.cod_municipio::text), 4, '0')
left join uor u on u.codigo = trim(c.cod_uor::text)
