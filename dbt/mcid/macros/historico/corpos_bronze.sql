{#
    Corpos das 13 bronzes de serie historica, uma por familia (D5).

    Cada modelo em models/**/bronze/ e uma casca fina que chama o corpo do seu
    dominio passando o nome da familia; o glob e os discriminadores vem do mapa
    em macros/historico/familias.sql. Assim as 13 tabelas nascem de um laco
    sobre o mapa, e nao de 13 corpos escritos a mao.

    Nenhum corpo usa `union all by name`, `select * exclude` ou
    `duckdb.query()` (tarefa 3.7): o motor DuckDB roda FORA do Postgres
    (D1) e a uniao entre familias vive na silver (D5).

    Destino parametrizado pelo target (D2): o mesmo corpo materializa no
    arquivo local (`--target staging_duckdb`) ou no Postgres atachado
    (`--target prod_duckdb`).
#}

{#- Serie executiva: 1 familia = 1 glob. `report_date` e `content_hash`
    existem nas 4 familias, entao o corpo e uniforme. -#}
{% macro bronze_serie_executiva(nome_familia) %}
{%- set f = familia(familias_serie_executiva(), nome_familia) -%}
with

    fonte as (
        select *, '{{ f.nome }}' as fonte_familia, filename as source_file
        from {{ read_minio_staging_parquet_series(f.glob) }}
    )

select
    *,
    {{ hist_snapshot_date_plausivel(parse_hist_date('report_date')) }}
        as report_date_parsed,
    {{ hist_dt_referencia('report_date', 'source_file') }} as dt_referencia,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|',
            source_file,
            coalesce(cast(content_hash as varchar), ''),
            cast(row_number() over (partition by source_file) as varchar)
        )
    ) as hash_linha
from fonte
{% endmacro %}


{#- GEFUS: 1 interface = 1 glob. Reentregas (sufixo != _YYYYMMDD) e arquivos
    de VALIDACAO ficam de fora, como antes. -#}
{% macro bronze_gefus(nome_familia) %}
{%- set f = familia(familias_gefus(), nome_familia) -%}
with

    fonte as (
        select
            *,
            '{{ f.fonte_interface }}' as fonte_interface,
            filename as source_file,
            strptime(regexp_extract(filename, '(\d{8})', 1), '%Y%m%d')::date
            as dt_referencia
        from {{ read_minio_staging_parquet_series(f.glob) }}
        where
            regexp_matches(filename, '_\d{8}\.parquet$')
            and filename not ilike '%validacao%'
    )

select
    *,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|',
            source_file,
            cast(row_number() over (partition by source_file) as varchar)
        )
    ) as hash_linha
from fonte
{% endmacro %}


{#- SNH empreendimento: 1 agente = 1 glob. O filtro `%entrega%` continua
    necessario porque o glob do CAIXA tambem casa os fluxos de entrega. -#}
{% macro bronze_snh_empreendimento(nome_familia) %}
{%- set f = familia(familias_snh_empreendimento(), nome_familia) -%}
with

    fonte as (
        select
            *,
            filename as source_file,
            strptime(
                regexp_extract(
                    regexp_replace(filename, '(20\d{2})_(\d{2})', '\1\2'), '(\d{6})', 1
                ),
                '%Y%m'
            )::date as dt_referencia,
            case
                when lower(filename) like '%af_bb%'
                then 'BB'
                when lower(filename) like '%af_caixa%'
                then 'CAIXA'
            end as agente_arquivo,
            case
                when lower(filename) like '%correcao%'
                then 3
                when regexp_matches(lower(filename), 'vs[0-9]+')
                then 2
                else 1
            end as prioridade_reentrega,
            current_timestamp as dt_ingest
        from {{ read_minio_staging_parquet_series(f.glob) }}
        where lower(filename) not like '%entrega%'
    )

select
    *,
    md5(
        concat_ws(
            '|',
            coalesce(agente_financeiro::text, agente_arquivo, ''),
            coalesce(apf::text, ''),
            coalesce(dt_referencia::text, ''),
            coalesce(modalidade::text, ''),
            coalesce(uh_contratadas::text, ''),
            coalesce(uh_entregues::text, ''),
            coalesce(uh_vigentes::text, '')
        )
    ) as hash_linha
from fonte
{% endmacro %}


{#- SNH entregas por evento: 1 agente = 1 glob. Aqui, diferente das outras
    tres familias, as colunas de data e de quantidade TEM nome diferente por
    agente (CAIXA: dt_entrega/qt_uh_entregues; BB: dt_ass_doc/
    numero_de_unidades_entregues) — o que o `union_by_name` da bronze unica
    escondia. Com uma tabela por agente, cada corpo so pode referenciar as
    colunas do seu proprio lote: dai o `coalesce_present_cols` sobre as
    colunas reais do glob. -#}
{% macro bronze_snh_entregas(nome_familia) %}
{%- set f = familia(familias_snh_entregas(), nome_familia) -%}
{%- set cols = staging_glob_columns(f.glob) -%}
{%- set dt_evento = coalesce_present_cols(cols, ['dt_entrega', 'dt_ass_doc'], try_cast_as='date') -%}
{%- set qt_evento = coalesce_present_cols(cols, ['qt_uh_entregues', 'numero_de_unidades_entregues']) -%}
{%- set dt_evento_txt = coalesce_present_cols(cols, ['dt_entrega', 'dt_ass_doc'], try_cast_as='varchar') -%}
{%- set qt_evento_txt = coalesce_present_cols(cols, ['qt_uh_entregues', 'numero_de_unidades_entregues'], try_cast_as='varchar') -%}
with

    fonte as (
        select
            *,
            filename as source_file,
            {{ hist_dt_referencia('report_date', 'filename') }} as dt_referencia,
            case
                when lower(filename) like '%af_caixa%'
                then 'CAIXA'
                when lower(filename) like '%af_b%'
                then 'BB'
            end as agente_arquivo,
            current_timestamp as dt_ingest
        from {{ read_minio_staging_parquet_series(f.glob) }}
    )

select
    *,
    -- helpers harmonizados (tipagem/normalizacao fina fica na silver)
    {{ dt_evento }} as dt_evento,
    {{ parse_hist_bigint(qt_evento) }} as qt_uh_entregues_evento,
    md5(
        concat_ws(
            '|',
            coalesce(cast(agente_financeiro as varchar), agente_arquivo, ''),
            coalesce(cast(apf as varchar), ''),
            coalesce({{ dt_evento_txt }}, ''),
            coalesce({{ qt_evento_txt }}, ''),
            coalesce(cast(dt_referencia as varchar), '')
        )
    ) as hash_linha
from fonte
{% endmacro %}
