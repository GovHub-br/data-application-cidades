{#-
  Teste genérico: falha se alguma das 3 colunas de PII de mutuário
  (macros/historico/pii_mutuario.sql — nu_cpf_cnpj_mutuario, no_mutuario,
  dt_nascimento_mutuario) aparecer no schema materializado do model. Confere
  o D4 da change frentes-restantes-mcmv-historico (PII fica só na bronze).

  Nível: error (não é warning de qualidade — é vazamento de dado pessoal).

  Uso no schema.yml (nível de model):

      models:
        - name: prata_hist_classe_media_contrato
          data_tests:
            - pii_mutuario_ausente
-#}
{% macro test_pii_mutuario_ausente(model) %}

{%- set proibidas = colunas_pii_mutuario() -%}
{%- set atuais = [] -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(model) -%}
        {%- do atuais.append(c.name | lower) -%}
    {%- endfor -%}
{%- endif -%}

with atual as (
    {%- if atuais | length > 0 %}
    {%- for c in atuais %}
    select '{{ c }}' as coluna{% if not loop.last %}
    union all{% endif %}
    {%- endfor %}
    {%- else %}
    select cast(null as varchar) as coluna where 1 = 0
    {%- endif %}
)

select coluna
from atual
where coluna in (
    {%- for p in proibidas -%}
    '{{ p }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
)

{% endmacro %}
