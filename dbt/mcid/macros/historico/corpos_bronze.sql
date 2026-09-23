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


{#- Frentes novas do GEFUS (change frentes-restantes-mcmv-historico): Classe
    Média, MCMV Cidades, Reforma Casa Brasil. Snapshots `_YYYY_MM_DD.parquet`
    sob a mesma pasta GEFUS das interfaces INT0xx. O schema cru dessas 3
    familias JA TRAZ uma coluna `dt_referencia` (texto DD/MM/YYYY, por linha)
    -- colide de nome com a coluna de auditoria do dominio. Preservada como
    `dt_referencia_origem_txt` (valor intacto, so desambiguada); a auditoria
    `dt_referencia` e SEMPRE a data do NOME DO ARQUIVO, mesma razao de
    confiabilidade das interfaces INT0xx (issue-130). Usa
    `staging_glob_columns` (introspeccao em tempo de compilacao) em vez de
    `select * exclude`, pela mesma convencao dos demais corpos deste arquivo. -#}
{% macro bronze_frente_gefus_semanal(nome_familia) %}
{%- set f = familia(familias_frentes_gefus(), nome_familia) -%}
{%- set cols = staging_glob_columns(f.glob) -%}
{%- set outras = cols | reject('in', ['dt_referencia', 'filename']) | list -%}
with

    fonte as (
        select
            {% for c in outras -%}
            {{ c }},
            {% endfor -%}
            {%- if 'dt_referencia' in cols -%}
            dt_referencia as dt_referencia_origem_txt,
            {%- endif %}
            '{{ f.frente }}' as frente_mcmv,
            filename as source_file,
            -- extrai a PRIMEIRA data YYYY_MM_DD do nome, não ancorada ao fim:
            -- reentregas trazem sufixo extra depois da data (ex.
            -- `..._2026_03_27_0000.parquet`), que uma âncora `\.parquet$`
            -- deixaria sem casar (regexp_extract vazio -> strptime falhava).
            try_strptime(regexp_extract(filename, '(\d{4}_\d{2}_\d{2})', 1), '%Y_%m_%d')
            ::date as dt_referencia
        from {{ read_minio_staging_parquet_series(f.glob) }}
    )

select
    *,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|', source_file, cast(row_number() over (partition by source_file) as varchar)
        )
    ) as hash_linha
from fonte
{% endmacro %}


{#- Fonte fiel FLAT do sharepoint (change frentes-restantes-mcmv-historico):
    arquivo unico consolidado, sem snapshot datado no nome (novo_mcmv_cidades_
    emendas, reforma_casa_brasil_contratacao, contratos/empreendimentos/
    dominio do Canal FGTS, propostas SUB50 apresentadas/selecionadas).
    dt_referencia = data de ingestao do arquivo (`_ingested_at`, coluna que a
    ingestao upstream ja grava em toda tabela do sharepoint) -- nao ha
    snapshot semanal/mensal para derivar do nome. Quando a fonte ja traz uma
    coluna `dt_referencia` propria (caso de reforma_casa_brasil_contratacao),
    ela e preservada como `dt_referencia_origem_txt`, mesma convencao do
    corpo acima. -#}
{% macro bronze_flat_shpt(object_path) %}
{%- set cols = [] -%}
{%- if execute -%}
    {%- set res = run_query(
        'describe select * from ' ~ read_minio_staging_parquet(object_path)
    ) -%}
    {%- if res is not none -%}
        {%- for r in res.rows -%}{%- do cols.append(r[0] | lower) -%}{%- endfor -%}
    {%- endif -%}
{%- endif -%}
{%- set outras = cols | reject('equalto', 'dt_referencia') | list -%}
with

    fonte as (
        select
            {% for c in outras -%}
            {{ c }},
            {% endfor -%}
            {%- if 'dt_referencia' in cols -%}
            dt_referencia as dt_referencia_origem_txt,
            {%- endif %}
            '{{ object_path }}' as source_file,
            try_cast(_ingested_at as timestamptz)::date as dt_referencia
        from {{ read_minio_staging_parquet(object_path) }}
    )

select
    *,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|', coalesce(cast(_source_hash as varchar), ''), cast(row_number() over () as varchar)
        )
    ) as hash_linha
from fonte
{% endmacro %}


{#- Evolucao de obra por empreendimento (MONIT_MOV_OBRA): 1 frente = 1 glob.
    Copia fiel dos snapshots `_MENSAL_YYYYMM` (glob recursivo sob
    `sharepoint/Novo MCMV - */`, selecao pela frente NO NOME DO ARQUIVO).
    `_LAYOUT_` reforcado no where. dt_referencia = 1o dia do mes do sufixo
    `_YYYYMM`. Grao da fonte: 1 linha por `nu_apf` por arquivo -- a dedup por
    (apf, dt_referencia) mantendo o snapshot mais recente fica na silver. As 3
    frentes tem schemas DIVERGENTES (FAR: dt_movimento/co_situacao_obra; FDS/
    RURAL: dh_movimento/co_situacao_operacao) -- o bronze nao harmoniza, so
    empilha; a projecao explicita por braco fica em
    o braço `obra_mensal` das 3 pratas de frente. Change:
    enriquecer-quantidades-uh-e-sinais-obra-historico (D4). -#}
{% macro bronze_obra_mensal(nome_familia) %}
{%- set f = familia(familias_obra_mensal(), nome_familia) -%}
with

    fonte as (
        select
            *,
            '{{ f.frente }}' as frente_mcmv,
            filename as source_file,
            strptime(
                regexp_extract(filename, 'MENSAL_(\d{6})', 1), '%Y%m'
            )::date as dt_referencia,
            current_timestamp as dt_ingest
        from {{ read_minio_staging_parquet_series(f.glob) }}
        where filename not ilike '%_LAYOUT_%'
    )

select
    *,
    md5(
        concat_ws(
            '|',
            source_file,
            cast(row_number() over (partition by source_file) as varchar)
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
