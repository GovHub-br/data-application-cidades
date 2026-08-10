{#
    ===========================================================================
    Silver -- financiamentos pessoa fisica (CCI + CCA + pro-cotista)
    ===========================================================================
    PADROES EXERCITADOS AQUI
      - UNION ALL de tres arquivos distintos, cada um com o proprio join de
        dominio, unificados sob um mesmo contrato de colunas
      - literal de origem por ramo, para nao perder a procedencia
      - nome de coluna divergente entre origens: as duas primeiras usam
        faixa_de_renda_codigo, a terceira usa faixa -- alinhado no select
      - filtro de chave ausente em CADA ramo do union: o padrao vale por ramo,
        nao no final -- filtrar so depois deixaria o ramo sujo entrar

    O union all e o caso em que mais compensa ficar dentro do bloco DuckDB:
    os tres arquivos sao lidos e combinados numa passada so, sem ida e volta
    ao Postgres.

    Recorte fiel do silver_fgts_financiamentos_pf de producao.

    PADRAO DA CAMADA SILVER
    Atributo, boolean e contagem nunca saem nulos. MEDIDA e DATA preservam o
    nulo -- zero em medida distorce media e minimo sem dar erro, e a decisao de
    exibir zero e da gold. Timestamp e sempre timestamptz, interpretado no fuso
    de origem. As macros de macros/silver_padroes.sql sao aplicadas na projecao
    externa. Ver docs/padrao-camada-silver.md.
#}

{{
    config(
        enabled=var('fgts_poc_enabled', false),
        materialized='table',
        schema='fgts_poc',
        tags=['fgts_poc'],
        pre_hook='set duckdb.force_execution = true'
    )
}}

select
    {{ silver_codigo("r['numero_do_contrato']::varchar") }}::varchar    as numero_do_contrato,
    {{ silver_codigo("r['faixa_de_renda_codigo']::varchar") }}::varchar as faixa_de_renda_codigo,
    {{ silver_texto("r['faixa_renda_descricao']::varchar") }}::varchar as faixa_renda_descricao,
    {{ silver_texto("r['origem_pf']::varchar") }}::varchar             as origem_pf,
    {{ silver_medida("r['valor_financiamento']::numeric(15, 2)") }}     as valor_financiamento,

    -- dt_ingest fica nulo ate a auditoria de staging ser propagada
    null::timestamptz  as dt_ingest,
    current_timestamp  as dt_silver

from duckdb.query(
$DBTSTG$

    with cci as (
        select * from {{ staging_parquet('fgts_staging', 'cci_analitico') }}
    ),
    cca as (
        select * from {{ staging_parquet('fgts_staging', 'cca_analitico') }}
    ),
    pro_cotista as (
        select * from {{ staging_parquet('fgts_staging', 'pro_cotista') }}
    ),
    faixas as (
        select * from {{ staging_parquet('fgts_staging', 'faixa_de_renda') }}
    )

    select
        c.numero_do_contrato,
        c.faixa_de_renda_codigo,
        coalesce(f.faixa_de_renda, 'Não Informado') as faixa_renda_descricao,
        'CCI' as origem_pf,
        {{ parse_financial_value('c.vlr_do_financiamento', engine='duckdb') }} as valor_financiamento
    from cci c
    left join faixas f on c.faixa_de_renda_codigo = f.codigo
    where c.numero_do_contrato is not null and trim(c.numero_do_contrato) <> ''

    union all

    select
        a.numero_do_contrato,
        a.faixa_de_renda_codigo,
        coalesce(f.faixa_de_renda, 'Não Informado') as faixa_renda_descricao,
        'CCA' as origem_pf,
        {{ parse_financial_value('a.vlr_do_financiamento', engine='duckdb') }} as valor_financiamento
    from cca a
    left join faixas f on a.faixa_de_renda_codigo = f.codigo
    where a.numero_do_contrato is not null and trim(a.numero_do_contrato) <> ''

    union all

    -- origem com nome de coluna divergente: `faixa` em vez de
    -- `faixa_de_renda_codigo`, e a grafia `financiameto` do proprio arquivo
    select
        p.numero_do_contrato,
        p.faixa as faixa_de_renda_codigo,
        coalesce(f.faixa_de_renda, 'Não Informado') as faixa_renda_descricao,
        'PRO-COTISTA' as origem_pf,
        {{ parse_financial_value('p.vlr_do_financiameto_bruto', engine='duckdb') }} as valor_financiamento
    from pro_cotista p
    left join faixas f on p.faixa = f.codigo
    where p.numero_do_contrato is not null and trim(p.numero_do_contrato) <> ''

$DBTSTG$
) r
