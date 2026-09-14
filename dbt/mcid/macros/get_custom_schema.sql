-- Gerador de schema do projeto.
--
-- Antes delegava direto pro `generate_schema_name_for_env` nativo do dbt, que só
-- aplica os `+schema:` do dbt_project.yml quando `target.name == 'prod'` -- em
-- qualquer outro target TUDO colapsa no `schema:` do próprio target. Com a
-- entrada dos targets DuckDB (o eixo histórico é SQL nativo do DuckDB e roda em
-- `prod_duckdb` / `staging_duckdb`), isso quebrava de duas formas:
--
--   1. as seeds e os modelos do histórico caíam todos num schema só, em vez de
--      `seeds` / `bronze` / `prata` / `ouro`;
--   2. o histórico, ao referenciar um modelo materializado pelo target `prod`
--      (a `prata_fds_dim_empreendimento`, no Postgres atachado como `cidades`),
--      procurava no schema errado.
--
-- A lista abaixo faz os três targets de produção roteaarem schema do mesmo jeito,
-- então uma tabela mora no mesmo endereço independente de por qual motor foi
-- construída. O comportamento do target `prod` é idêntico ao de antes.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set targets_com_schema_custom = ['prod', 'prod_duckdb', 'staging_duckdb'] -%}
    {%- if target.name in targets_com_schema_custom and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
