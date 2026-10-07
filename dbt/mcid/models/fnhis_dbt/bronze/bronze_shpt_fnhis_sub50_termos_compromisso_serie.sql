{{ config(materialized="table") }}

-- Bronze: Termos de compromisso do FNHIS Sub-50 no TransfereGov — TODOS os retratos mensais
-- (a bronze_shpt_fnhis_sub50_termos_compromisso guarda só o mais recente).
-- Fonte: SHPT — sharepoint/Novo MCMV - FNHIS Sub 50/(Arquivados/)FNHIS SUB 50 Painel_TG_<AAAAMMDD>
--
-- Mesmo problema descrito na bronze do retrato atual: várias remessas trazem colunas de anexo
-- de e-mail com nome repetido, e ler o glob com `union_by_name` derruba a conexão. Aqui a lista
-- de arquivos e as colunas de cada um são resolvidas na COMPILAÇÃO (parquet_schema via
-- duckdb.query); cada arquivo é lido sozinho, projetando só as colunas de negócio, e coluna que
-- não existe num retrato antigo vira nulo. Tudo texto — tipagem é da prata.
{%- set padrao = "s3://data-lake-mcid/staging/**/FNHIS SUB 50 Painel_TG_*.parquet" -%}
{%- set colunas = [
    "no_proposta", "no_reservado_pac", "situacao_proposta", "sit_contratacao",
    "situacao_instrumento", "valor_de_repasse", "valor_de_contrapartida",
    "valor_empenhado_acumulado", "data_assinatura", "data_consulta", "uf", "municipio",
] -%}
{%- set arquivos = [] -%}
{%- set cols_por_arquivo = {} -%}
{%- if execute -%}
    {%- set lista = run_query(
        "select * from duckdb.query($q$select file from glob('" ~ padrao ~ "') order by file$q$) r"
    ) -%}
    {%- for linha in lista.rows -%}
        {%- set arq = linha[0] -%}
        {%- do arquivos.append(arq) -%}
        {%- set sch = run_query(
            "select * from duckdb.query($q$select name from parquet_schema('" ~ arq ~ "')$q$) r"
        ) -%}
        {%- do cols_por_arquivo.update({arq: sch.columns[0].values() | map('lower') | list}) -%}
    {%- endfor -%}
    {%- if arquivos | length == 0 -%}
        {{ exceptions.raise_compiler_error("nenhum arquivo casou com " ~ padrao) }}
    {%- endif -%}
{%- endif %}

{% if not execute %}
select null::varchar as filename
{% else %}
{% for arq in arquivos %}
select
    '{{ arq }}'::varchar as filename,
    {%- for c in colunas %}
    {% if c in cols_por_arquivo[arq] -%}
    cast(r['{{ c }}'] as varchar)
    {%- else -%}
    null::varchar
    {%- endif %} as {{ c }}{{ "," if not loop.last }}
    {%- endfor %}
from read_parquet('{{ arq }}') as r
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endif %}
