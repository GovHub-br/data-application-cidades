{#
    ===========================================================================
    Silver -- empreendimentos do FGTS
    ===========================================================================
    PADROES EXERCITADOS AQUI
      - join de quatro arquivos parquet numa unica passada
      - PIVOT: a tabela de GPS vem em formato longo (uma linha por
        caracteristica) e vira duas colunas via max(case when ...) + group by
      - nullif('None') para tratar o texto literal que a origem grava no lugar
        de nulo -- sem isso o valor 'None' passa como se fosse dado valido
      - coalesce com default, para nao propagar nulo
      - parse_int com default

    DIFERENCA EM RELACAO AO silver_fgts_empreendimentos DE PRODUCAO
    O model de producao tambem le fgts_sftp_empreendimentos_base_pj, que hoje
    NAO existe na camada de staging do MinIO. Esse CTE foi removido aqui. Com
    ele voltariam dois padroes: coalesce entre fontes concorrentes
    (coalesce(c.cgc, sp.cgc, ...)) e join por expressao
    (right(sp.nu_apf, 7) = e.cod_empreendimento).

    Para reativar quando o objeto existir na staging: declarar a tabela no
    source fgts_staging com o nome real do arquivo (convencao do bucket:
    <nome_origem>.csv.parquet), acrescentar o CTE sftp_pj, o left join por
    expressao, os dois braços de coalesce e as colunas no group by.

    Como sempre, a transformacao inteira vive dentro do bloco duckdb.query:
    fora dele so vale o que o parser do Postgres entende.

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
    {{ silver_codigo("r['cod_empreendimento']::varchar") }}::varchar  as cod_empreendimento,
    {{ silver_texto("r['nome_empreendimento']::varchar") }}::varchar as nome_empreendimento,
    {{ silver_texto("r['objeto']::varchar") }}::varchar              as objeto,
    {{ silver_texto("r['localidade']::varchar") }}::varchar          as localidade,
    {{ silver_texto("r['logradouro']::varchar") }}::varchar          as logradouro,

    {{ silver_texto("r['cnpj_construtora']::varchar") }}::varchar    as cnpj_construtora,
    {{ silver_texto("r['nome_construtora']::varchar") }}::varchar    as nome_construtora,

    {{ silver_codigo("r['cod_municipio']::varchar") }}::varchar       as cod_municipio,
    {{ silver_texto("r['municipio_nome']::varchar") }}::varchar      as municipio_nome,
    {{ silver_uf("r['municipio_uf']::varchar") }}::varchar           as municipio_uf,

    {{ silver_texto("r['gps_lat']::varchar") }}::varchar             as gps_lat,
    {{ silver_texto("r['gps_long']::varchar") }}::varchar            as gps_long,

    {{ silver_medida_inteira("r['quantidade_uh']::integer") }}              as quantidade_uh,

    -- dt_ingest fica nulo ate a auditoria de staging ser propagada
    null::timestamptz  as dt_ingest,
    current_timestamp  as dt_silver

from duckdb.query(
$DBTSTG$

    with empreendimentos as (
        select * from {{ staging_parquet('fgts_staging', 'empreendimentos') }}
    ),
    construtor as (
        select * from {{ staging_parquet('fgts_staging', 'empreendimentos_construtor') }}
    ),
    gps as (
        select * from {{ staging_parquet('fgts_staging', 'empreendimentos_gps') }}
    ),
    municipios as (
        select * from {{ staging_parquet('fgts_staging', 'municipios') }}
    )

    select
        e.cod_empreendimento,
        coalesce(e.txt_nome_empreendimento, 'Não Informado') as nome_empreendimento,
        e.txt_objeto      as objeto,
        e.txt_localidade  as localidade,
        e.txt_logradouro  as logradouro,

        -- nullif('None') tambem aqui: a origem grava a string literal 'None'
        -- no lugar de nulo, e sem tratar isso ela vaza como CNPJ valido
        coalesce(nullif(c.cgc, 'None'), 'Não Informado')      as cnpj_construtora,
        coalesce(nullif(c.entidade, 'None'), 'Não Informado') as nome_construtora,

        e.cod_municipio,
        coalesce(m.municipio, 'Não Informado') as municipio_nome,
        coalesce(m.uf, 'ND')                   as municipio_uf,

        -- PIVOT do formato longo para colunas. O nullif por dentro do max
        -- descarta o 'None' literal antes da agregacao, senao ele pode ganhar
        -- do valor real dependendo da ordenacao.
        max(
            case
                when g.tipo_da_caracteristica = 'GPS LATITUDE GRAU'
                then nullif(g.texto, 'None')
            end
        ) as gps_lat,
        max(
            case
                when g.tipo_da_caracteristica = 'GPS LONGITUDE GRAU'
                then nullif(g.texto, 'None')
            end
        ) as gps_long,

        coalesce({{ parse_int('e.qtd_unidades_financiadas', engine='duckdb') }}, 0) as quantidade_uh

    from empreendimentos e
    left join municipios m  on e.cod_municipio = m.codigo
    left join construtor c  on e.cod_empreendimento = c.cod_empreendimento
    left join gps g         on e.cod_empreendimento = g.empreendimento_codigo

    -- chave ausente nao entra na silver (padrao, regra 1)
    where e.cod_empreendimento is not null
      and trim(e.cod_empreendimento) <> ''

    group by
        e.cod_empreendimento, e.txt_nome_empreendimento, e.txt_objeto,
        e.txt_localidade, e.txt_logradouro, c.cgc, c.entidade,
        e.cod_municipio, m.municipio, m.uf, e.qtd_unidades_financiadas

$DBTSTG$
) r
