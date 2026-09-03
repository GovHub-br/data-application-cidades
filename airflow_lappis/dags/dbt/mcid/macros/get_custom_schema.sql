-- Gerador de schema do projeto.
--
-- Comportamento:
--   * target `prod`  -> `generate_schema_name_for_env` usa o `+schema` custom
--                       como está (schemas globais bronze/silver/gold, conjuntura, ...).
--   * target `duckdb` (staging_duckdb) -> honra o `+schema` custom literalmente,
--                       para que o padrão de schemas globais (bronze/silver/gold)
--                       também valha ao ler o MinIO. Sem esse ramo o
--                       `generate_schema_name_for_env` padrão jogaria tudo no
--                       `target.schema` único.
--   * demais targets de dev -> delega ao padrão (`<target>_<schema>`).
--
-- Coordenado com a change `migracao-bronze-minio-mcmv` (decisão D4): as duas
-- mudanças convergem para esta mesma macro; quem aplicar depois apenas valida.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none and target.type == 'duckdb' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ generate_schema_name_for_env(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
