{#-
  Grava em lake._dbt_log o que não passou numa execução do dbt.

  Roda no on-run-end, onde o dbt expõe `results` com o desfecho de cada model e cada
  teste. Só entra no log o que não passou: execução limpa não escreve linha nenhuma.

  Existe por causa do teste de drift, que é `severity: warn` de propósito — drift de
  layout na origem não deve derrubar a DAG, mas também não pode passar despercebido.
  Com o log, dá para responder "houve drift?" com um select, sem abrir log de task
  do Airflow.

  A leitura do que aconteceu:

      select * from lake._dbt_log order by executado_em desc limit 20;
-#}
{% macro registrar_execucao_dbt() %}

    {%- if not execute or results is not defined -%} {{ return("") }} {%- endif -%}

    {%- set schema_lake = env_var("LAKE_SCHEMA", "lake") -%}

    {%- set ddl -%}
        create schema if not exists {{ schema_lake }};
        create table if not exists {{ schema_lake }}._dbt_log (
            id serial primary key,
            invocation_id text,
            executado_em timestamptz default now(),
            recurso text,
            modelo text,
            tipo text,
            status text,
            divergencias bigint,
            mensagem text,
            detalhe text
        );
        -- `modelo` entrou depois da primeira versão da tabela.
        alter table {{ schema_lake }}._dbt_log add column if not exists modelo text;
        alter table {{ schema_lake }}._dbt_log add column if not exists detalhe text;
    {%- endset -%}
    {%- do run_query(ddl) -%}

    {%- set pendentes = [] -%}
    {%- for r in results -%}
        {%- if r.status | string not in ["success", "pass"] -%}
            {%- do pendentes.append(r) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if pendentes -%}
        {%- set linhas = [] -%}
        {%- for r in pendentes -%}
            {%- set mensagem = (r.message | string | replace("'", "''"))[:500] -%}

            {#- Em teste genérico, `name` é o nome longo que o dbt monta com todos os
                argumentos. O que serve para ler o log é o tipo do teste e o model
                testado, então os dois saem em colunas separadas. -#}
            {%- if r.node.resource_type | string == "test" and r.node.test_metadata is defined -%}
                {%- set recurso = r.node.test_metadata.name -%}
            {%- else -%} {%- set recurso = r.node.name -%}
            {%- endif -%}

            {%- set alvos = [] -%}
            {%- for dep in r.node.depends_on.nodes -%}
                {%- if dep.startswith("model.") -%}
                    {%- do alvos.append(dep.split(".") | last) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- set modelo = alvos | join(", ") if alvos else r.node.name -%}

            {#- Um teste que falha devolve as linhas do problema, mas o `results` só
                traz a CONTAGEM delas. Reexecutar o SQL já compilado do teste é o que
                põe no log qual coluna divergiu, em vez de só "houve 1 divergência".
                Só para teste, e só quando houve falha: model com erro não roda aqui. -#}
            {%- set detalhe = "" -%}
            {%- if r.node.resource_type | string == "test" and r.failures and r.node.compiled_code -%}
                {%- set achados = run_query(r.node.compiled_code) -%}
                {%- set partes = [] -%}
                {%- for linha in achados.rows[:10] -%}
                    {%- do partes.append(linha | join(" ")) -%}
                {%- endfor -%}
                {%- set detalhe = (partes | join(" ; ") | replace("'", "''"))[:1000] -%}
            {%- endif -%}

            {%- do linhas.append(
                "('"
                ~ invocation_id
                ~ "', '"
                ~ recurso
                ~ "', '"
                ~ modelo
                ~ "', '"
                ~ r.node.resource_type
                ~ "', '"
                ~ r.status
                ~ "', "
                ~ (r.failures if r.failures is not none else "null")
                ~ ", '"
                ~ mensagem
                ~ "', '"
                ~ detalhe
                ~ "')"
            ) -%}
        {%- endfor -%}

        {%- set insert -%}
            insert into {{ schema_lake }}._dbt_log
                (invocation_id, recurso, modelo, tipo, status, divergencias,
                 mensagem, detalhe)
            values {{ linhas | join(", ") }};
        {%- endset -%}
        {%- do run_query(insert) -%}
        {%- do log(
            "Registradas " ~ pendentes
            | length ~ " ocorrências em " ~ schema_lake ~ "._dbt_log",
            info=true,
        ) -%}
    {%- endif -%}

    {{ return("") }}

{% endmacro %}
