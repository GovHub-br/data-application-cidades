{{
    config(
        materialized='incremental',
        unique_key=['schema_name', 'table_name'],
        on_schema_change='sync_all_columns'
    )
}}

{#
    Tabela de Metadados dos Modelos dbt (Projeto MCID)
    ==================================================
    
    Esta tabela armazena metadados de todos os modelos executados no dbt para o pipeline do MCID.
    É fundamental para a governança de dados e para mostrar a "Data de Última Atualização" nos dashboards.
    
    A tabela é atualizada de forma incremental, mantendo apenas o registro
    mais recente para cada combinação de schema + table_name.
#}

WITH dbt_models AS (
    {% set models_data = [] %}
    
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% do models_data.append({
                'schema_name': node.schema,
                'table_name': node.name,
                'database_name': node.database,
                'materialization': node.config.materialized,
                'description': node.description | default('') | replace("'", "''")
            }) %}
        {% endif %}
    {% endfor %}

    {% for model in models_data %}
        SELECT
            '{{ model.schema_name }}' AS schema_name,
            '{{ model.table_name }}' AS table_name,
            '{{ model.database_name }}' AS database_name,
            '{{ model.materialization }}' AS materialization,
            '{{ model.description[:500] }}' AS description,
            ('{{ run_started_at }}'::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dt_transform,
            '{{ invocation_id }}' AS run_id
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    schema_name,
    table_name,
    database_name,
    materialization,
    description,
    dt_transform,
    run_id
FROM dbt_models
