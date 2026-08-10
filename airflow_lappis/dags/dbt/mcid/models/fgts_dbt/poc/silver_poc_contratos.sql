{#
    ===========================================================================
    Silver -- contratos do FGTS, lidos direto da staging (MinIO/parquet)
    ===========================================================================
        dbt run --select tag:fgts_poc --vars '{fgts_poc_enabled: true}'

    NAO ha camada bronze. A staging no MinIO ja e a bronze da medallion --
    copia fiel do raw, em parquet, tudo em text -- e materializa-la de novo no
    Postgres seria duplicar a mesma camada.

    ---------------------------------------------------------------------------
    POR QUE TUDO ESTA DENTRO DE duckdb.query($$ ... $$)
    ---------------------------------------------------------------------------
    O pg_duckdb e uma extensao do Postgres, entao o parser do Postgres ve o SQL
    antes de qualquer pushdown. Na pratica isso significa que, FORA de um bloco
    duckdb.query, so da para usar o que o Postgres entende: a sintaxe precisa
    ser valida e as funcoes precisam existir no catalogo. Descoberto na marra,
    em tres execucoes:

      1. to_timestamp(texto, mascara)  -> Binder Error do DuckDB: a query
         inteira tinha sido empurrada, e la essa funcao nao existe.
      2. ['a','b']                     -> syntax error no parser do Postgres:
         literal de lista e sintaxe do DuckDB.
      3. try_strptime(...)             -> function does not exist: nem funcoes
         do DuckDB passam, so as que o pg_duckdb declara (read_parquet etc).

    Dentro do bloco $$ ... $$ nada disso se aplica: e uma string, o Postgres nao
    interpreta, e o DuckDB recebe SQL nativo. Por isso os joins, os casts e toda
    a padronizacao ficam la dentro.

    A camada de fora e so projecao: r['coluna']::tipo, que e a sintaxe de
    subscrito que o proprio pg_duckdb expoe.

    ---------------------------------------------------------------------------
    Padronizacao
    ---------------------------------------------------------------------------
    Feita na leitura, com try_strptime e try_cast -- fail-soft, devolvem NULL em
    vez de abortar, o que dispensa testar o formato antes de converter. Valor
    financeiro irreconhecivel vira 0.00; data irreconhecivel vira NULL.
    Ver macros/parsers.sql e macros/parse_financial_value.sql, ramo 'duckdb'.

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
    {{ silver_codigo("r['cod_contrato']::varchar") }}::varchar         as cod_contrato,
    {{ silver_codigo("r['cod_empreendimento']::varchar") }}::varchar   as cod_empreendimento,
    {{ silver_codigo("r['cod_tomador']::varchar") }}::varchar          as cod_tomador,

    {{ silver_codigo("r['cod_area']::varchar") }}::varchar             as cod_area,
    {{ silver_texto("r['area_descricao']::varchar") }}::varchar       as area_descricao,
    {{ silver_codigo("r['cod_linha']::varchar") }}::varchar            as cod_linha,
    {{ silver_texto("r['linha_descricao']::varchar") }}::varchar      as linha_descricao,
    {{ silver_codigo("r['cod_modalidade']::varchar") }}::varchar       as cod_modalidade,
    {{ silver_texto("r['modalidade_descricao']::varchar") }}::varchar as modalidade_descricao,

    {{ silver_timestamp("r['data_assinatura']::timestamp") }}              as data_assinatura,
    {{ silver_medida_inteira("r['ano_orcamento']::integer") }}               as ano_orcamento,

    {{ silver_medida("r['valor_contratado']::numeric(15, 2)") }}       as valor_contratado,
    {{ silver_medida("r['valor_investimento']::numeric(15, 2)") }}     as valor_investimento,

    {{ silver_texto("r['status_contrato']::varchar") }}::varchar      as status_contrato,

    -- dt_ingest fica nulo ate a auditoria de staging ser propagada
    null::timestamptz  as dt_ingest,
    current_timestamp  as dt_silver

from duckdb.query(
$DBTSTG$

    with contratos as (
        select * from {{ staging_parquet('fgts_staging', 'contratos') }}
    ),

    areas as (
        select * from {{ staging_parquet('fgts_staging', 'area') }}
    ),

    linhas as (
        select * from {{ staging_parquet('fgts_staging', 'linha') }}
    ),

    modalidades as (
        select * from {{ staging_parquet('fgts_staging', 'modalidade') }}
    )

    select
        c.cod_contrato,
        c.cod_empreendimento,
        c.cod_tomador,

        c.cod_area,
        coalesce(a.area, 'Não Informado')             as area_descricao,
        c.cod_linha,
        coalesce(l.linha, 'Não Informado')            as linha_descricao,
        c.cod_modalidade,
        coalesce(m.modalidade, 'Não Informado')       as modalidade_descricao,

        {{ parse_timestamp('c.dte_assinatura', engine='duckdb') }}          as data_assinatura,
        {{ parse_int('c.dte_orcamento_ano', engine='duckdb') }}             as ano_orcamento,

        {{ parse_financial_value('c.vlr_contratado', engine='duckdb') }}    as valor_contratado,
        {{ parse_financial_value('c.vlr_investimento', engine='duckdb') }}  as valor_investimento,

        coalesce(c.cod_situacao_contrato, 'Não Informado') as status_contrato

    from contratos c
    left join areas a       on c.cod_area = a.codigo
    left join linhas l      on c.cod_linha = l.codigo
    left join modalidades m on c.cod_modalidade = m.codigo

    -- chave ausente nao entra na silver (padrao, regra 1)
    where c.cod_contrato is not null
      and trim(c.cod_contrato) <> ''

$DBTSTG$
) r
